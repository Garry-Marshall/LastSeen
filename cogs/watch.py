"""Watchlist / activity-alert cog.

Admin-configured presence alerts. Two alert types cover every use case:

  * online_return — edge-triggered from the offline->online transition. Fires
    once per return (naturally), optionally gated on a minimum time-away, with a
    per-watch cooldown to suppress rapid presence flicker.
  * offline_for  — swept hourly; fires once when a target crosses an offline
    threshold, re-arming when the target next comes online.

Targets are a user or a role. Everything reads the existing single
members.last_seen column; the watchlists table stores only alert config plus
minimal fire-state, never presence history.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional, Union

import discord
from discord import app_commands
from discord.ext import commands, tasks

from database import DatabaseManager
from bot.utils import (
    create_embed,
    create_error_embed,
    create_success_embed,
    has_bot_admin_role,
    chunk_list,
)
from cogs.commands import PaginationView
from bot.locale import t, guild_language

logger = logging.getLogger(__name__)

# Alert delivery / abuse bounds
MAX_WATCHES_PER_GUILD = 50
ONLINE_COOLDOWN_SECONDS = 3600      # suppress re-firing an online_return watch within 1h
MIN_DURATION_SECONDS = 5 * 60       # 5m
MAX_DURATION_SECONDS = 365 * 86400  # 365d
# The offline_for sweep runs on this cadence, so it also bounds how late an
# alert can fire past its threshold. Kept small (near the 5m minimum threshold)
# rather than hourly; the sweep's cost scales with the number of offline_for
# watches (bounded, admin-created), not with the user count.
OFFLINE_SWEEP_INTERVAL_SECONDS = 60
WATCHES_PER_PAGE = 10
# After a (re)connect, Discord resyncs presence and can surface spurious
# offline->online transitions. Suppress online_return alerts for this long after
# each on_ready so a restart (or reconnect) doesn't spam the channel. offline_for
# needs no such window — its armed/triggered + fired_targets state is persisted.
STARTUP_GRACE_SECONDS = 120

_DURATION_RE = re.compile(r'^\s*(\d+)\s*([mhd])\s*$', re.IGNORECASE)
_UNIT_SECONDS = {'m': 60, 'h': 3600, 'd': 86400}


def parse_duration(text: str) -> Optional[int]:
    """Parse a compact duration string ('30m', '48h', '7d') into seconds.

    Returns None if the format is unrecognised. Range is not enforced here.
    """
    match = _DURATION_RE.match(text or '')
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2).lower()
    if value <= 0:
        return None
    return value * _UNIT_SECONDS[unit]


def format_duration(seconds: int) -> str:
    """Render seconds back to the largest exact compact unit ('7d', '48h', '30m')."""
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    return f"{max(seconds // 60, 0)}m"


class WatchCog(commands.Cog):
    """Watchlist commands, the online-return listener, and the offline sweep."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        self.bot = bot
        self.db = db
        self.config = config
        # Per-(watch_id, user_id) last-fired timestamps for the online_return
        # cooldown. In-memory and per-member (a role watch must not suppress one
        # member's return because another member of the role just returned), and
        # transient — losing it on restart at worst allows one extra alert.
        # Pruned in the hourly sweep so it stays bounded.
        self._online_cooldown: dict = {}
        # Suppress online_return firing until this unix time. Seeded to cover the
        # startup window (the cog loads before connect) and refreshed on every
        # on_ready so reconnects are covered too.
        self._suppress_online_until = int(datetime.now(timezone.utc).timestamp()) + STARTUP_GRACE_SECONDS
        # Keep the shared hot-path index in sync with what's actually stored.
        self._refresh_watch_guilds()
        self.check_offline_watches.start()

    @commands.Cog.listener()
    async def on_ready(self):
        """Re-arm the online_return suppression window on every (re)connect."""
        self._suppress_online_until = int(datetime.now(timezone.utc).timestamp()) + STARTUP_GRACE_SECONDS
        logger.info(f"WatchCog: suppressing online-return alerts for {STARTUP_GRACE_SECONDS}s after (re)connect")

    def cog_unload(self):
        self.check_offline_watches.cancel()

    def _refresh_watch_guilds(self) -> None:
        """Rebuild the in-memory set of guilds that have watches (hot-path gate)."""
        self.bot.watch_guild_ids = self.db.get_watch_guild_ids()

    async def _ensure_admin(self, interaction: discord.Interaction):
        """Gate a command on the bot-admin role. Returns lang on success, else None."""
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        lang = guild_language(guild_config)
        role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        if not has_bot_admin_role(interaction.user, role_name):
            await interaction.response.send_message(
                embed=create_error_embed(t('errors.no_permission', lang, role=role_name), lang),
                ephemeral=True,
            )
            return None
        return lang

    # ==================== Commands ====================

    watch_group = app_commands.Group(
        name="watch",
        description="👀 Manage presence watches and activity alerts (Admin only)",
        guild_only=True,
    )

    async def _add_watch(self, interaction: discord.Interaction, lang: str,
                         target: Union[discord.Member, discord.Role],
                         alert_type: str, threshold_seconds: Optional[int],
                         channel: Optional[discord.TextChannel]) -> None:
        """Shared validation + persistence for the online/offline add commands."""
        # Resolve and validate the target. The mentionable option can hand back a
        # discord.Member, discord.User, discord.Role, or a bare discord.Object
        # (no .bot/.is_default/.mention), so validate defensively off the id and
        # the guild's own cached objects rather than trusting the resolved value.

        # @everyone — its role id equals the guild id. Check this up front so it's
        # caught whether the picker resolves it to a Role or a bare Object (Discord
        # often omits @everyone from resolved roles), and always gets the right
        # message instead of falling through to a later check.
        if target.id == interaction.guild.id:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.err_everyone', lang), lang), ephemeral=True)
            return

        if isinstance(target, discord.Role):
            # A bot's integration role (auto-created, named after the bot) is
            # selectable in the picker and is a real Role, so watching it is
            # effectively watching a bot. Prefer the cached role, whose tags are
            # complete, over the resolved value.
            role_obj = interaction.guild.get_role(target.id) or target
            if role_obj.is_bot_managed():
                await interaction.response.send_message(
                    embed=create_error_embed(t('watch.err_bot', lang), lang), ephemeral=True)
                return
            target_type, target_id = 'role', target.id
        else:  # discord.Member, discord.User, or a bare discord.Object
            # The mentionable value's own bot flag is unreliable, so cross-check
            # the guild's cached member, which carries the authoritative bot flag.
            cached = interaction.guild.get_member(target.id)
            if getattr(target, 'bot', False) or (cached is not None and cached.bot):
                await interaction.response.send_message(
                    embed=create_error_embed(t('watch.err_bot', lang), lang), ephemeral=True)
                return
            if target.id in self.bot.opted_out_users:
                await interaction.response.send_message(
                    embed=create_error_embed(t('watch.err_opted_out', lang), lang), ephemeral=True)
                return
            target_type, target_id = 'user', target.id

        dest = channel or interaction.channel

        # Enforce the per-guild cap, but never block re-configuring an existing watch.
        existing = await asyncio.to_thread(self.db.get_guild_watches, interaction.guild_id)
        already = any(
            w['target_type'] == target_type and w['target_id'] == target_id
            and w['alert_type'] == alert_type for w in existing
        )
        if not already and len(existing) >= MAX_WATCHES_PER_GUILD:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.err_cap', lang, max=MAX_WATCHES_PER_GUILD), lang),
                ephemeral=True)
            return

        seq = await asyncio.to_thread(
            self.db.add_watch, interaction.guild_id, target_type, target_id,
            alert_type, threshold_seconds, dest.id, interaction.user.id,
        )
        if seq is None:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.err_generic', lang), lang), ephemeral=True)
            return

        self._refresh_watch_guilds()

        # Build the confirmation. Derive the mentions from the ids/type rather than
        # target.mention: the target may be a bare discord.Object (no .mention).
        target_mention = f"<@&{target_id}>" if target_type == 'role' else f"<@{target_id}>"
        dest_mention = f"<#{dest.id}>"
        if alert_type == 'online_return':
            extra = t('watch.added_online_extra', lang, duration=format_duration(threshold_seconds)) \
                if threshold_seconds else ''
            desc = t('watch.added_online', lang, target=target_mention,
                     channel=dest_mention, extra=extra)
        else:
            desc = t('watch.added_offline', lang, target=target_mention,
                     channel=dest_mention, duration=format_duration(threshold_seconds))

        # Warn if the destination is undeliverable.
        me = interaction.guild.me
        if not dest.permissions_for(me).send_messages:
            desc += t('watch.warn_cannot_send', lang, channel=dest_mention)

        embed = create_embed(t('watch.added_title', lang), discord.Color.green())
        embed.description = desc
        embed.set_footer(text=f"#{seq}")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"Watch #{seq} ({alert_type}, {target_type}:{target_id}) created in guild {interaction.guild.name} by {interaction.user}")

    @watch_group.command(name="online", description="🔔 Alert when a member/role comes back online")
    @app_commands.describe(
        target="The user or role to watch",
        after="Only alert if they'd been away at least this long, e.g. 7d (optional)",
        channel="Where to post the alert (defaults to this channel)",
    )
    async def watch_online(self, interaction: discord.Interaction,
                           target: Union[discord.Member, discord.Role],
                           after: Optional[str] = None,
                           channel: Optional[discord.TextChannel] = None):
        lang = await self._ensure_admin(interaction)
        if lang is None:
            return
        threshold = None
        if after is not None:
            threshold = parse_duration(after)
            if threshold is None:
                await interaction.response.send_message(
                    embed=create_error_embed(t('watch.err_duration_format', lang), lang), ephemeral=True)
                return
            if not (MIN_DURATION_SECONDS <= threshold <= MAX_DURATION_SECONDS):
                await interaction.response.send_message(
                    embed=create_error_embed(t('watch.err_duration_range', lang,
                        min=format_duration(MIN_DURATION_SECONDS),
                        max=format_duration(MAX_DURATION_SECONDS)), lang), ephemeral=True)
                return
        await self._add_watch(interaction, lang, target, 'online_return', threshold, channel)

    @watch_group.command(name="offline", description="💤 Alert when a member/role has been offline for a duration")
    @app_commands.describe(
        target="The user or role to watch",
        duration="How long offline before alerting, e.g. 48h or 7d",
        channel="Where to post the alert (defaults to this channel)",
    )
    async def watch_offline(self, interaction: discord.Interaction,
                            target: Union[discord.Member, discord.Role],
                            duration: str,
                            channel: Optional[discord.TextChannel] = None):
        lang = await self._ensure_admin(interaction)
        if lang is None:
            return
        threshold = parse_duration(duration)
        if threshold is None:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.err_duration_format', lang), lang), ephemeral=True)
            return
        if not (MIN_DURATION_SECONDS <= threshold <= MAX_DURATION_SECONDS):
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.err_duration_range', lang,
                    min=format_duration(MIN_DURATION_SECONDS),
                    max=format_duration(MAX_DURATION_SECONDS)), lang), ephemeral=True)
            return
        await self._add_watch(interaction, lang, target, 'offline_for', threshold, channel)

    @watch_group.command(name="list", description="📋 List this server's watches")
    async def watch_list(self, interaction: discord.Interaction):
        lang = await self._ensure_admin(interaction)
        if lang is None:
            return
        watches = await asyncio.to_thread(self.db.get_guild_watches, interaction.guild_id)
        if not watches:
            embed = create_embed(t('watch.list_title', lang), discord.Color.blue())
            embed.description = t('watch.list_empty', lang)
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        lines = [self._format_watch_line(w, lang) for w in watches]
        embeds = []
        for i, page in enumerate(chunk_list(lines, WATCHES_PER_PAGE)):
            embed = create_embed(t('watch.list_title', lang), discord.Color.blue())
            embed.description = "\n".join(page)
            embeds.append(embed)

        if len(embeds) == 1:
            await interaction.response.send_message(embed=embeds[0], ephemeral=True)
        else:
            view = PaginationView(embeds, lang=lang)
            await interaction.response.send_message(embed=embeds[0], view=view, ephemeral=True)

    def _format_watch_line(self, w: dict, lang: str) -> str:
        target = f"<@&{w['target_id']}>" if w['target_type'] == 'role' else f"<@{w['target_id']}>"
        channel = f"<#{w['channel_id']}>" if w['channel_id'] else "?"
        if w['alert_type'] == 'online_return':
            extra = t('watch.list_extra_after', lang, duration=format_duration(w['threshold_seconds'])) \
                if w['threshold_seconds'] else ''
            return t('watch.list_line_online', lang, id=w['seq'], target=target,
                     extra=extra, channel=channel)
        state = t('watch.state_triggered', lang) if w['state'] == 'triggered' else t('watch.state_armed', lang)
        return t('watch.list_line_offline', lang, id=w['seq'], target=target,
                 duration=format_duration(w['threshold_seconds']), channel=channel, state=state)

    def _target_label(self, guild: discord.Guild, w: dict) -> str:
        """A readable name for a watch's target (cache-only; for lists/autocomplete)."""
        if w['target_type'] == 'role':
            role = guild.get_role(w['target_id'])
            return f"@{role.name}" if role else f"role {w['target_id']}"
        member = guild.get_member(w['target_id'])
        return f"@{member.display_name}" if member else f"user {w['target_id']}"

    async def _resolve_target(self, guild: discord.Guild, raw: str):
        """Resolve a free-text reference to a watch target.

        Accepts a user/role mention, a raw snowflake id, or a username/nickname
        (via find_member_by_name) / role name. Returns (target_type, target_id),
        where target_type is None for a bare snowflake (match either type by id),
        or (None, None) if nothing matched.
        """
        m = re.match(r'^<@!?(\d+)>$', raw)
        if m:
            return 'user', int(m.group(1))
        m = re.match(r'^<@&(\d+)>$', raw)
        if m:
            return 'role', int(m.group(1))
        if raw.isdigit():
            return None, int(raw)
        member = await asyncio.to_thread(self.db.find_member_by_name, guild.id, raw)
        if member:
            return 'user', member['user_id']
        role = discord.utils.find(lambda r: r.name.lower() == raw.lower(), guild.roles)
        if role:
            return 'role', role.id
        return None, None

    async def remove_autocomplete(self, interaction: discord.Interaction, current: str):
        """Suggest the guild's current watches, so remove is pickable by number or name."""
        if not interaction.guild:
            return []
        watches = await asyncio.to_thread(self.db.get_guild_watches, interaction.guild_id)
        cur = current.lower().lstrip('#')
        out = []
        for w in watches:
            label = self._target_label(interaction.guild, w)
            kind = 'online' if w['alert_type'] == 'online_return' else 'offline'
            name = f"#{w['seq']} · {kind} · {label}"[:100]
            if not cur or cur in str(w['seq']) or cur in name.lower():
                out.append(app_commands.Choice(name=name, value=str(w['seq'])))
            if len(out) >= 25:
                break
        return out

    @watch_group.command(name="remove", description="🗑️ Remove a watch by its number, or by the watched user/role")
    @app_commands.describe(target="A watch number from /watch list, or a username, nickname, user ID, or role")
    @app_commands.autocomplete(target=remove_autocomplete)
    async def watch_remove(self, interaction: discord.Interaction, target: str):
        lang = await self._ensure_admin(interaction)
        if lang is None:
            return
        guild = interaction.guild
        raw = target.strip().lstrip('#')
        watches = await asyncio.to_thread(self.db.get_guild_watches, interaction.guild_id)
        if not watches:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.list_empty', lang), lang), ephemeral=True)
            return

        # An existing per-guild watch number wins; otherwise resolve as a target
        # (removing every watch on that user/role). A user snowflake is also all
        # digits, but watch numbers are small and matched against real rows first.
        by_num = raw.isdigit() and any(w['seq'] == int(raw) for w in watches)
        if by_num:
            matched = [w for w in watches if w['seq'] == int(raw)]
        else:
            tgt_type, tgt_id = await self._resolve_target(guild, raw)
            matched = [w for w in watches
                       if tgt_id is not None and w['target_id'] == tgt_id
                       and (tgt_type is None or w['target_type'] == tgt_type)]

        if not matched:
            await interaction.response.send_message(
                embed=create_error_embed(t('watch.remove_not_found', lang, query=raw), lang), ephemeral=True)
            return

        removed = 0
        for w in matched:
            if await asyncio.to_thread(self.db.remove_watch, interaction.guild_id, w['id']):
                removed += 1
        self._refresh_watch_guilds()

        if by_num:
            msg = t('watch.removed', lang, id=int(raw))
        else:
            label = self._target_label(guild, matched[0])
            msg = t('watch.removed_target', lang, count=removed, target=label)
        await interaction.response.send_message(embed=create_success_embed(msg, lang), ephemeral=True)
        logger.info(f"Removed {removed} watch(es) ({[w['id'] for w in matched]}) in guild {interaction.guild.name} by {interaction.user}")

    # ==================== online_return firing + re-arm ====================

    @commands.Cog.listener()
    async def on_lastseen_member_online(self, member: discord.Member, previous_last_seen: Optional[int]):
        """Fire online_return watches for a returning member and re-arm their
        offline_for watches. Dispatched by TrackingCog with the pre-overwrite
        last_seen so the away-duration survives the two listeners racing.
        """
        if member.id in self.bot.opted_out_users:
            return

        guild_id = member.guild.id
        user_id = member.id
        role_ids = [r.id for r in member.roles if not r.is_default()]
        now = int(datetime.now(timezone.utc).timestamp())
        away = (now - previous_last_seen) if previous_last_seen and previous_last_seen > 0 else None

        try:
            # During the post-connect grace window, skip firing online_return
            # alerts — presence is resyncing and can surface spurious returns —
            # but still run the offline_for re-arm below so state stays correct.
            if now >= self._suppress_online_until:
                online = await asyncio.to_thread(
                    self.db.get_watches_for_member, guild_id, user_id, role_ids, 'online_return')
                for w in online:
                    threshold = w['threshold_seconds'] or 0
                    # Away-gated watches only fire on a confirmed long-enough absence.
                    if threshold and (away is None or away < threshold):
                        continue
                    # Per-member cooldown to suppress rapid presence flicker.
                    cd_key = (w['id'], user_id)
                    last_fired = self._online_cooldown.get(cd_key)
                    if last_fired and now - last_fired < ONLINE_COOLDOWN_SECONDS:
                        continue
                    await self._fire_alert(member.guild, w, member.mention, online_away=away if threshold else None)
                    self._online_cooldown[cd_key] = now

            # Re-arm offline_for watches for this member.
            offline = await asyncio.to_thread(
                self.db.get_watches_for_member, guild_id, user_id, role_ids, 'offline_for')
            for w in offline:
                if w['target_type'] == 'user':
                    if w['state'] != 'armed':
                        await asyncio.to_thread(self.db.update_watch_fire_state, w['id'], state='armed')
                else:  # role: drop this member from the already-alerted set
                    fired = set(json.loads(w['fired_targets'])) if w['fired_targets'] else set()
                    if user_id in fired:
                        fired.discard(user_id)
                        await asyncio.to_thread(self.db.update_watch_fire_state, w['id'],
                                                fired_targets=json.dumps(sorted(fired)))
        except Exception as e:
            logger.error(f"Error handling online return for {member} in guild {member.guild.name}: {e}", exc_info=True)

    # ==================== offline_for sweep ====================

    @tasks.loop(seconds=OFFLINE_SWEEP_INTERVAL_SECONDS)
    async def check_offline_watches(self):
        """Fire offline_for watches whose targets have crossed their threshold."""
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            # Drop online_return cooldown entries that have expired, bounding the map.
            cutoff = now - ONLINE_COOLDOWN_SECONDS
            self._online_cooldown = {k: v for k, v in self._online_cooldown.items() if v >= cutoff}

            watches = await asyncio.to_thread(self.db.get_offline_watches)
            if not watches:
                return
            for w in watches:
                try:
                    guild = self.bot.get_guild(w['guild_id'])
                    if not guild or not w['threshold_seconds']:
                        continue
                    if w['target_type'] == 'user':
                        await self._sweep_user_watch(guild, w, now)
                    else:
                        await self._sweep_role_watch(guild, w, now)
                except Exception as e:
                    logger.error(f"Error sweeping watch #{w['id']}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in check_offline_watches: {e}", exc_info=True)

    @check_offline_watches.before_loop
    async def before_check_offline_watches(self):
        await self.bot.wait_until_ready()

    async def _sweep_user_watch(self, guild: discord.Guild, w: dict, now: int):
        member_data = await asyncio.to_thread(self.db.get_member, guild.id, w['target_id'])
        if not member_data:
            return
        last_seen = member_data['last_seen']
        # last_seen: 0 = online, None = never seen. Re-arm on return; only fire
        # on a real offline timestamp that has crossed the threshold.
        if not last_seen:  # 0 or None
            if w['state'] != 'armed':
                await asyncio.to_thread(self.db.update_watch_fire_state, w['id'], state='armed')
            return
        if w['state'] == 'triggered':
            return
        if now - last_seen >= w['threshold_seconds']:
            await self._fire_alert(guild, w, f"<@{w['target_id']}>", offline_for=w['threshold_seconds'])
            await asyncio.to_thread(self.db.update_watch_fire_state, w['id'], state='triggered')

    async def _sweep_role_watch(self, guild: discord.Guild, w: dict, now: int):
        role = guild.get_role(w['target_id'])
        if not role:
            return
        members = [m for m in role.members if not m.bot and m.id not in self.bot.opted_out_users]
        if not members:
            return
        last_seen_map = await asyncio.to_thread(
            self.db.get_members_last_seen, guild.id, [m.id for m in members])

        old_fired = set(json.loads(w['fired_targets'])) if w['fired_targets'] else set()
        new_fired = set()
        newly = []
        for m in members:
            last_seen = last_seen_map.get(m.id)
            if last_seen and now - last_seen >= w['threshold_seconds']:
                new_fired.add(m.id)
                if m.id not in old_fired:
                    newly.append(m)
        if newly:
            listing = "\n".join(f"• {m.mention}" for m in newly)
            await self._fire_alert(guild, w, role.mention, role_offline=w['threshold_seconds'], members=listing)
        if new_fired != old_fired:
            await asyncio.to_thread(self.db.update_watch_fire_state, w['id'],
                                    fired_targets=json.dumps(sorted(new_fired)))

    # ==================== delivery ====================

    async def _fire_alert(self, guild: discord.Guild, w: dict, target_mention: str, *,
                          online_away: Optional[int] = None,
                          offline_for: Optional[int] = None,
                          role_offline: Optional[int] = None,
                          members: Optional[str] = None):
        """Build and post one alert embed to the watch's channel."""
        channel = guild.get_channel_or_thread(w['channel_id']) if w['channel_id'] else None
        if not channel:
            logger.warning(f"Watch #{w['id']}: channel {w['channel_id']} not found in guild {guild.name}; can't deliver")
            return
        if not channel.permissions_for(guild.me).send_messages:
            logger.warning(f"Watch #{w['id']}: no send permission in channel {channel.id}; can't deliver")
            return

        lang = guild_language(await asyncio.to_thread(self.db.get_guild_config, guild.id))

        if role_offline is not None:
            title = t('watch.alert_offline_title', lang)
            desc = t('watch.alert_role_offline_desc', lang, role=target_mention,
                     duration=format_duration(role_offline), members=members or '')
            color = discord.Color.orange()
        elif offline_for is not None:
            title = t('watch.alert_offline_title', lang)
            desc = t('watch.alert_offline_desc', lang, target=target_mention,
                     duration=format_duration(offline_for))
            color = discord.Color.orange()
        else:
            title = t('watch.alert_online_title', lang)
            if online_away:
                desc = t('watch.alert_online_desc_away', lang, target=target_mention,
                         duration=format_duration(online_away))
            else:
                desc = t('watch.alert_online_desc', lang, target=target_mention)
            color = discord.Color.green()

        embed = create_embed(title, color)
        embed.description = desc
        try:
            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Watch #{w['id']}: failed to send alert to channel {channel.id}: {e}")


async def setup(bot: commands.Bot):
    db = bot.db
    config = bot.config
    await bot.add_cog(WatchCog(bot, db, config))
    logger.info("WatchCog loaded")

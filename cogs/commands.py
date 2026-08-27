"""User commands cog for querying member information."""

import asyncio
import discord
from discord import app_commands
from discord.ext import commands
import logging
import json
import re
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta

from database import DatabaseManager
from bot.utils import (
    parse_user_mention,
    create_embed,
    create_error_embed,
    create_success_embed,
    format_timestamp,
    chunk_list,
    can_use_bot_commands,
    is_channel_allowed,
    has_bot_admin_role,
    format_health_delta,
    compute_community_pulse,
    format_pct_arrow
)
from bot.locale import t, guild_language, weekday_name

logger = logging.getLogger(__name__)


def sanitize_csv_value(value) -> str:
    """Sanitize a value for CSV export.

    Strips newlines and neutralizes spreadsheet formula injection: cells
    starting with =, +, -, @ or a tab are interpreted as formulas by Excel
    and similar tools, so prefix them with a single quote.
    """
    value = str(value).replace('\n', ' ').replace('\r', ' ')
    if value and value[0] in ('=', '+', '-', '@', '\t'):
        value = "'" + value
    return value


def build_activity_sparkline(trend: list, days: int = 30) -> str:
    """Render a per-day message-activity strip as a monospace sparkline.

    `trend` is a get_message_activity_trend() result: sparse rows of
    {'date': <UTC day-start>, 'message_count': n}. We fill the gaps so every
    one of the last `days` days is represented, most recent on the right.
    Silent days show as '·' so genuine inactivity reads as visible gaps; active
    days scale ▁..█ relative to the member's own busiest day in the window.
    """
    counts = {row['date']: row['message_count'] for row in trend}

    now = datetime.now(timezone.utc)
    today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())

    series = [counts.get(today_start - (offset * 86400), 0)
              for offset in range(days - 1, -1, -1)]

    peak = max(series)
    if peak == 0:
        return "·" * days

    ramp = "▁▂▃▄▅▆▇█"
    chars = []
    for count in series:
        if count == 0:
            chars.append("·")
        else:
            level = (count * (len(ramp) - 1) + peak - 1) // peak
            chars.append(ramp[level])
    return "".join(chars)


class PaginationView(discord.ui.View):
    """Interactive pagination view for navigating through multiple pages."""

    def __init__(self, embeds: list[discord.Embed], timeout: int = 180,
                 export_members: list[dict] = None, lang: str = 'en'):
        """
        Initialize pagination view.

        Args:
            embeds: List of embeds to paginate through
            timeout: Timeout in seconds (default 3 minutes)
            export_members: Optional member dicts to enable a CSV export button
            lang: Language code for the export button responses
        """
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.current_page = 0
        self.max_pages = len(embeds)
        self.export_members = export_members
        self.lang = lang

        # Disable buttons if only one page
        if self.max_pages == 1:
            self.first_page_button.disabled = True
            self.prev_button.disabled = True
            self.next_button.disabled = True
            self.last_page_button.disabled = True

        # Only show the export button when there is data to export
        if not export_members:
            self.remove_item(self.export_csv_button)

    @discord.ui.button(label="⏮️", style=discord.ButtonStyle.secondary)
    async def first_page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to first page."""
        self.current_page = 0
        await self._update_message(interaction)

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        self.current_page = max(0, self.current_page - 1)
        await self._update_message(interaction)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        self.current_page = min(self.max_pages - 1, self.current_page + 1)
        await self._update_message(interaction)

    @discord.ui.button(label="⏭️", style=discord.ButtonStyle.secondary)
    async def last_page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to last page."""
        self.current_page = self.max_pages - 1
        await self._update_message(interaction)

    @discord.ui.button(label="📄 Export CSV", style=discord.ButtonStyle.green)
    async def export_csv_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export the members as a CSV file."""
        await interaction.response.defer(ephemeral=True)
        try:
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                'Username', 'Nickname', 'User ID', 'Last Seen', 'Joined', 'Roles'
            ], quoting=csv.QUOTE_ALL)  # Quote all fields for safety
            writer.writeheader()
            for member in self.export_members:
                last_seen = member.get('last_seen')
                join_date = member.get('join_date')
                writer.writerow({
                    'Username': sanitize_csv_value(member.get('username') or ''),
                    'Nickname': sanitize_csv_value(member.get('nickname') or ''),
                    'User ID': member['user_id'],
                    'Last Seen': datetime.fromtimestamp(last_seen, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if last_seen else '',
                    'Joined': datetime.fromtimestamp(join_date, tz=timezone.utc).strftime('%Y-%m-%d') if join_date else '',
                    'Roles': sanitize_csv_value(', '.join(str(r) for r in member.get('roles', []))),
                })

            filename = f"inactive_members_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            file = discord.File(fp=StringIO(output.getvalue()), filename=filename)
            await interaction.followup.send(
                t("commands.search.export_success", self.lang, count=len(self.export_members), format="CSV"),
                file=file,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(t("commands.search_view.export_failed", self.lang, error=e), ephemeral=True)

    async def _update_message(self, interaction: discord.Interaction):
        """Update the message with current page and button states."""
        # Update button states
        self.first_page_button.disabled = (self.current_page == 0)
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page == self.max_pages - 1)
        self.last_page_button.disabled = (self.current_page == self.max_pages - 1)

        await interaction.response.edit_message(
            embed=self.embeds[self.current_page],
            view=self
        )


class ForgetMeConfirmView(discord.ui.View):
    """Confirmation view for the /forgetme privacy opt-out."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, user_id: int, lang: str = 'en'):
        super().__init__(timeout=60)
        self.bot = bot
        self.db = db
        self.user_id = user_id
        self.lang = lang
        self.confirm_button.label = t("commands.forgetme.btn_confirm", lang)
        self.cancel_button.label = t("common.cancel", lang)

    @discord.ui.button(label="Yes, delete my data", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Opt the user out, then purge their data everywhere."""
        lang = self.lang
        await interaction.response.defer()

        # Opt out FIRST so no event re-creates rows while the purge runs
        self.bot.opted_out_users.add(self.user_id)
        if not await asyncio.to_thread(self.db.add_opted_out_user, self.user_id):
            self.bot.opted_out_users.discard(self.user_id)
            await interaction.edit_original_response(
                embed=create_error_embed(t("commands.forgetme.save_failed", lang), lang),
                view=None
            )
            self.stop()
            return

        # Drop any buffered activity entries before they reach the database
        tracking_cog = self.bot.get_cog('TrackingCog')
        if tracking_cog:
            tracking_cog.purge_user_from_buffers(self.user_id)

        counts = await asyncio.to_thread(self.db.purge_user_data, self.user_id)
        if counts is None:
            await interaction.edit_original_response(
                embed=create_error_embed(t("commands.forgetme.delete_failed", lang), lang),
                view=None
            )
            self.stop()
            return

        activity_total = counts['message_activity'] + counts['message_activity_hourly']
        await interaction.edit_original_response(
            embed=create_success_embed(
                t("commands.forgetme.success", lang,
                  members=counts['members'],
                  roles=counts['role_changes'],
                  activity=activity_total),
                lang
            ),
            view=None
        )
        logger.info(f"User {self.user_id} opted out of tracking and purged their data")
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the opt-out."""
        lang = self.lang
        embed = create_embed(t("common.cancelled", lang), discord.Color.blue())
        embed.description = t("commands.forgetme.cancelled_desc", lang)
        await interaction.response.edit_message(embed=embed, view=None)
        self.stop()


class CommandsCog(commands.Cog):
    """Cog for user-facing commands."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        """
        Initialize commands cog.

        Args:
            bot: Discord bot instance
            db: Database manager
            config: Bot configuration
        """
        self.bot = bot
        self.db = db
        self.config = config

    async def _check_permissions(self, interaction: discord.Interaction) -> tuple[bool, discord.Embed | None, bool, str]:
        """
        Check if user has permission to use commands in this channel.

        Args:
            interaction: Discord interaction

        Returns:
            Tuple of (can_proceed, error_embed, channels_restricted)
            channels_restricted: True if allowed_channels is configured (means output should not be ephemeral)
        """
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        channels_restricted = False
        lang = guild_language(guild_config)

        # Check role permissions
        if guild_config and not can_use_bot_commands(interaction.user, guild_config):
            user_role_name = guild_config.get('user_role_name', 'LastSeen User')
            error = create_error_embed(
                t("errors.no_permission", lang, role=user_role_name), lang
            )
            return False, error, channels_restricted, lang

        # Check channel permissions and determine if channels are restricted
        if guild_config:
            allowed_channels_json = guild_config.get('allowed_channels')
            channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False
            if not is_channel_allowed(interaction.channel_id, guild_config):
                error = create_error_embed(
                    t("errors.channel_not_allowed", lang), lang
                )
                return False, error, channels_restricted, lang

        return True, None, channels_restricted, lang

    async def user_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        """
        Autocomplete callback for user parameters.
        Suggests member names as user types.
        
        Args:
            interaction: Discord interaction
            current: Current user input
            
        Returns:
            List of autocomplete choices
        """
        if not interaction.guild:
            return []
        
        # Get all members from database for this guild
        members = await asyncio.to_thread(self.db.get_guild_members, interaction.guild_id, False)
        
        # Filter based on current input (case-insensitive)
        current_lower = current.lower()
        matches = []
        
        for member in members:
            username = member.get('username', '')
            nickname = member.get('nickname', '')
            
            # Match on username or nickname
            if current_lower in username.lower() or (nickname and current_lower in nickname.lower()):
                # Show nickname if available, otherwise username
                display_name = nickname if nickname else username
                matches.append(app_commands.Choice(name=display_name[:100], value=username[:100]))
                
            # Limit to 25 choices (Discord limit)
            if len(matches) >= 25:
                break
        
        return matches

    @app_commands.command(name="whois", description="👤 Get information about a user")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def whois(self, interaction: discord.Interaction, user: str):
        """
        Display information about a user.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = await asyncio.to_thread(self.db.find_member_by_name, guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.whois.not_found", lang), lang),
                ephemeral=not channels_restricted
            )
            return

        # Get Discord member object for additional info (online status, boosting, etc.)
        member = interaction.guild.get_member(member_data['user_id'])

        # Check if caller is admin - get bot admin role name from guild config
        guild_config = await asyncio.to_thread(self.db.get_guild_config, guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        is_admin = has_bot_admin_role(interaction.user, bot_admin_role_name)

        # Create embed with new formatting
        username = member_data['username'] if member_data['username'] else t("common.unknown", lang)
        embed = create_embed(t("commands.whois.title", lang, username=username), discord.Color.blue())
        embed.description = ""

        # ===== USER IDENTITY SECTION =====
        embed.description += t("commands.whois.user_id", lang, user_id=member_data['user_id'])

        # Account creation date - admin only
        if is_admin and member and hasattr(member, 'created_at'):
            try:
                account_created = format_timestamp(int(member.created_at.timestamp()), 'F', guild_id, self.db, lang)
                embed.description += t("commands.whois.account_created", lang, date=account_created)
            except (AttributeError, ValueError, OSError):
                pass

        # Join info with position
        if member_data['join_date']:
            join_str = format_timestamp(member_data['join_date'], 'F', guild_id, self.db, lang)
            join_position = member_data.get('join_position')
            if join_position:
                embed.description += t("commands.whois.joined_with_position", lang, date=join_str, position=join_position)
            else:
                embed.description += t("commands.whois.joined", lang, date=join_str)

        embed.description += "\n"

        # ===== NICKNAME & ROLES SECTION =====
        nickname = member_data['nickname'] if member_data['nickname'] else t("common.not_set", lang)
        embed.description += t("commands.whois.nickname", lang, nickname=nickname)

        # Nickname history - admin only (exclude current nickname)
        if is_admin and member_data.get('nickname_history'):
            try:
                history = json.loads(member_data['nickname_history'])
                if history:
                    # Filter out the current nickname to show only previous ones
                    previous_nicknames = [n for n in history if n != nickname]
                    if previous_nicknames:
                        history_str = ", ".join(previous_nicknames)
                        embed.description += t("commands.whois.previously_known", lang, names=history_str)
            except (json.JSONDecodeError, TypeError):
                pass

        # Highest role
        if member and hasattr(member, 'roles') and member.roles and len(member.roles) > 1:  # > 1 because everyone has @everyone
            try:
                highest_role = member.top_role
                if highest_role and highest_role.name != "@everyone":
                    embed.description += t("commands.whois.highest_role", lang, role=highest_role.mention)
            except (AttributeError, IndexError):
                pass

        embed.description += "\n"

        # ===== STATUS SECTION =====
        if member:
            # Online status
            status_emoji = {
                discord.Status.online: "🟢",
                discord.Status.idle: "🟡",
                discord.Status.dnd: "🔴",
                discord.Status.offline: "⚫"
            }.get(member.status, "⚫")

            embed.description += t("commands.whois.status", lang, emoji=status_emoji, status=str(member.status).capitalize())

            # Last seen
            if hasattr(member, 'status') and member.status != discord.Status.offline:
                embed.description += t("commands.whois.last_seen_online", lang)
            elif member_data['last_seen'] and member_data['last_seen'] != 0:
                embed.description += t("commands.whois.last_seen", lang, date=format_timestamp(member_data['last_seen'], 'R', guild_id, self.db, lang))
            else:
                embed.description += t("commands.whois.last_seen_unavailable", lang)
        else:
            embed.description += t("commands.whois.status_left", lang)

        # Boosting status
        if member and hasattr(member, 'premium_since') and member.premium_since:
            try:
                boost_date = format_timestamp(int(member.premium_since.timestamp()), 'F', guild_id, self.db, lang)
                embed.description += t("commands.whois.boosting_yes", lang, date=boost_date)
            except (AttributeError, ValueError, OSError):
                pass
        else:
            embed.description += t("commands.whois.boosting_no", lang)

        embed.description += "\n"

        # ===== MESSAGE ACTIVITY SECTION =====
        activity_stats = await asyncio.to_thread(self.db.get_message_activity_period, guild_id, member_data['user_id'], days=30)
        if activity_stats and (activity_stats['total'] > 0 or activity_stats['today'] >= 0):
            embed.description += t("commands.whois.activity_header", lang)
            embed.description += t("commands.whois.activity_today", lang, count=activity_stats['today'])
            embed.description += t("commands.whois.activity_week", lang, count=activity_stats['this_week'])
            embed.description += t("commands.whois.activity_month", lang, count=activity_stats['this_month'])
            if activity_stats['avg_per_day'] > 0:
                embed.description += t("commands.whois.activity_avg", lang, avg=activity_stats['avg_per_day'])

        # ===== ACTIVITY PROFILE SECTION =====
        # When-active patterns are more revealing than raw counts, so this block
        # is limited to admins and to a member viewing their own profile. It is
        # derived entirely from message activity (never presence).
        show_profile = is_admin or interaction.user.id == member_data['user_id']
        if show_profile:
            guild_tz = guild_config.get('timezone', 'UTC') if guild_config else 'UTC'
            panel = await asyncio.to_thread(
                self.db.get_whois_activity_panel, guild_id, member_data['user_id'], 30, guild_tz
            )
            profile = panel['profile']
            if profile['total'] > 0:
                embed.description += "\n"
                embed.description += t("commands.whois.profile_header", lang)
                embed.description += t("commands.whois.profile_active_days", lang,
                                       active=profile['active_days'], total=profile['window_days'])

                # 30-day activity strip: shows whether absences are constant or
                # occasional at a glance. Built from message activity only.
                embed.description += t("commands.whois.profile_timeline", lang,
                                       sparkline=build_activity_sparkline(panel['trend'], 30))

                # Suppress the pattern lines on thin data — a "most active day"
                # off a handful of messages is noise, not a profile.
                if profile['total'] >= 15 and profile['active_days'] >= 5:
                    if profile['peak_weekday'] is not None:
                        embed.description += t("commands.whois.profile_peak_day", lang,
                                               day=weekday_name(profile['peak_weekday'], lang))
                    if profile['peak_hours']:
                        start, end = profile['peak_hours']
                        embed.description += t("commands.whois.profile_peak_hours", lang,
                                               start=f"{start:02d}:00", end=f"{end:02d}:00")
                    trend_key = {
                        'increasing': "commands.whois.profile_trend_up",
                        'decreasing': "commands.whois.profile_trend_down",
                        'steady': "commands.whois.profile_trend_steady",
                    }.get(profile['trend'])
                    if trend_key:
                        embed.description += t(trend_key, lang)

                    percentile = panel['percentile']
                    if percentile:
                        filled = round(percentile['percentile'] / 10)
                        bar = "█" * filled + "░" * (10 - filled)
                        embed.description += t("commands.whois.profile_activity_bar", lang,
                                               bar=bar, percentile=percentile['percentile'])

        # Journey button — only for viewers allowed to see the profile above.
        view = discord.utils.MISSING
        if show_profile:
            view = JourneyView(self, guild_id, member_data['user_id'], username, lang)
        await interaction.followup.send(embed=embed, view=view, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /whois for '{user}' in guild {interaction.guild.name}")

    async def _lastseen_impl(self, interaction: discord.Interaction, user: str, command_name: str):
        """
        Shared implementation for lastseen/seen commands.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
            command_name: Name of command that called this (for logging)
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = await asyncio.to_thread(self.db.find_member_by_name, guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.whois.not_found", lang), lang),
                ephemeral=not channels_restricted
            )
            return

        # Create embed
        embed = create_embed(t("commands.lastseen.title", lang), discord.Color.green())

        embed.add_field(
            name=t("commands.lastseen.field_username", lang),
            value=member_data['username'] if member_data['username'] else t("common.not_set", lang),
            inline=False
        )
        embed.add_field(
            name=t("commands.lastseen.field_nickname", lang),
            value=member_data['nickname'] if member_data['nickname'] else t("common.not_set", lang),
            inline=False
        )

        # Check if user is currently online
        member = interaction.guild.get_member(member_data['user_id'])
        if member and member.status != discord.Status.offline:
            embed.add_field(name=t("commands.lastseen.field_status", lang), value=t("commands.lastseen.currently_online", lang), inline=False)
            embed.add_field(name=t("commands.lastseen.field_last_seen", lang), value=t("commands.lastseen.right_meow", lang), inline=False)
        else:
            embed.add_field(name=t("commands.lastseen.field_status", lang), value=t("commands.lastseen.offline", lang), inline=False)
            if member_data['last_seen'] and member_data['last_seen'] != 0:
                embed.add_field(
                    name=t("commands.lastseen.field_last_seen", lang),
                    value=format_timestamp(member_data['last_seen'], 'R', guild_id, self.db, lang),
                    inline=False
                )
                embed.add_field(
                    name=t("commands.lastseen.field_exact_time", lang),
                    value=format_timestamp(member_data['last_seen'], 'F', guild_id, self.db, lang),
                    inline=False
                )
            else:
                embed.add_field(name=t("commands.lastseen.field_last_seen", lang), value=t("commands.lastseen.not_available", lang), inline=False)

        # Add status if inactive
        if member_data['is_active'] == 0:
            embed.add_field(name=t("commands.lastseen.field_note", lang), value=t("commands.lastseen.left_server", lang), inline=False)

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /{command_name} for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="lastseen", description="👁️ Check when a user was last seen online")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def lastseen(self, interaction: discord.Interaction, user: str):
        """
        Display when a user was last seen online.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        await self._lastseen_impl(interaction, user, "lastseen")

    @app_commands.command(name="seen", description="👁️ Alias for /lastseen - Check when a user was last seen")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def seen(self, interaction: discord.Interaction, user: str):
        """
        Alias for lastseen command.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        await self._lastseen_impl(interaction, user, "seen")

    @app_commands.command(name="role-history", description="📜 View role change history for a member (Admin only)")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def role_history(self, interaction: discord.Interaction, user: str):
        """
        Display role change history for a member (admin only).

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        # Check if user is admin
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(t("errors.no_permission", lang, role=bot_admin_role_name), lang),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = await asyncio.to_thread(self.db.find_member_by_name, guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.role_history.not_found", lang), lang),
                ephemeral=not channels_restricted
            )
            return

        # Get role history
        role_changes = await asyncio.to_thread(self.db.get_role_history, guild_id, member_data['user_id'], limit=20)

        if not role_changes:
            embed = create_embed(t("commands.role_history.title", lang, username=member_data['username']), discord.Color.blue())
            embed.description = t("commands.role_history.no_changes", lang)
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Create embed with role history
        username = member_data['username'] if member_data['username'] else t("common.unknown", lang)
        embed = create_embed(t("commands.role_history.title", lang, username=username), discord.Color.blue())
        embed.description = t("commands.role_history.header", lang)

        for change in role_changes:
            role_name = change.get('role_name', 'Unknown')
            action = change.get('action', 'unknown')
            timestamp = change.get('timestamp', 0)

            # Validate action value
            if action not in ("added", "removed"):
                logger.warning(f"Invalid action '{action}' in role_changes for {member_data['user_id']}")
                continue

            # Escape markdown in role name (avoid embed formatting issues)
            escaped_role_name = discord.utils.escape_markdown(role_name) if role_name else "Unknown"

            # Format action with emoji
            action_emoji = "➕" if action == "added" else "➖"
            action_text = t("commands.role_history.added", lang) if action == "added" else t("commands.role_history.removed", lang)
            time_str = format_timestamp(timestamp, 'R', guild_id, self.db, lang)

            embed.description += t("commands.role_history.line", lang, emoji=action_emoji, action=action_text, role=escaped_role_name, time=time_str)

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /role-history for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="inactive", description="💤 List inactive members — Admin only (add days to override threshold)")
    @app_commands.describe(days="Override default threshold - specify number of days (1-365)")
    @app_commands.checks.cooldown(1, 10.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def inactive(self, interaction: discord.Interaction, days: int = None):
        """
        List all members who have been inactive for longer than the configured threshold.

        Args:
            interaction: Discord interaction
            days: Optional override for inactive days threshold
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        # Admin-only: listing inactive members is a server-management action
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(t("errors.no_permission", lang, role=bot_admin_role_name), lang),
                ephemeral=True
            )
            return

        # Validate days input if provided
        if days is not None:
            if not (1 <= days <= 365):
                await interaction.response.send_message(
                    embed=create_error_embed(t("commands.inactive.validate_range", lang), lang),
                    ephemeral=True
                )
                return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        # Get guild config
        guild_id = interaction.guild_id
        guild_config = await asyncio.to_thread(self.db.get_guild_config, guild_id)
        if not guild_config:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.inactive.no_guild_config", lang), lang),
                ephemeral=True
            )
            return

        # Use provided days or fall back to config
        inactive_days = days if days is not None else guild_config['inactive_days']

        # Get inactive members, then restrict to the guild's track_only_roles
        # filter (applied at read time — every member is stored regardless).
        inactive_members = await asyncio.to_thread(self.db.get_inactive_members, guild_id, inactive_days)
        tracked = await asyncio.to_thread(self.db.get_tracked_user_ids, guild_id)
        if tracked is not None:
            tracked_set = set(tracked)
            inactive_members = [m for m in inactive_members if m['user_id'] in tracked_set]

        logger.info(f"User {interaction.user} used /inactive in guild {interaction.guild.name} with threshold {inactive_days}")
        logger.info(f"Found {len(inactive_members)} inactive members (>{inactive_days} days)")

        if not inactive_members:
            embed = create_embed(t("commands.inactive.empty_title", lang), discord.Color.blue())
            embed.description = t("commands.inactive.empty_desc", lang, days=inactive_days)
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Create paginated embeds (8 members per page)
        chunks = chunk_list(inactive_members, 8)
        embeds = []

        for i, chunk in enumerate(chunks):
            embed = create_embed(
                t("commands.inactive.page_title", lang, days=inactive_days, count=len(inactive_members), page=i + 1, total=len(chunks)),
                discord.Color.blue()
            )

            for member_data in chunk:
                # Create a field for each member
                username = member_data['username'] if member_data['username'] else t("common.unknown", lang)
                nickname = member_data['nickname'] if member_data['nickname'] else t("common.not_set", lang)
                last_seen = format_timestamp(member_data['last_seen'], 'R', guild_id, self.db, lang) if member_data['last_seen'] else t("common.never", lang)

                member_info = t("commands.inactive.member_info", lang, nickname=nickname, last_seen=last_seen)
                embed.add_field(name=username, value=member_info, inline=False)

            embeds.append(embed)

        # Send with pagination view
        view = PaginationView(embeds, export_members=inactive_members, lang=lang)
        await interaction.followup.send(embed=embeds[0], view=view, ephemeral=not channels_restricted)

    @app_commands.command(name="chat-history", description="📈 View extended message activity history (365 days)")
    @app_commands.describe(user="Username, nickname, or @mention (leave empty for server-wide stats)")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def chat_history(self, interaction: discord.Interaction, user: str = None):
        """
        Display extended message activity history.
        Shows user stats if specified, or guild-wide stats if not.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention (optional)
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        
        # If no user provided, show guild-wide stats
        if user is None:
            stats = await asyncio.to_thread(self.db.get_guild_message_activity_stats, guild_id, days=365)
            
            if stats['total_365d'] == 0:
                embed = create_embed(t("commands.chat_history.server_title", lang, guild=interaction.guild.name), discord.Color.blue())
                embed.description = t("commands.chat_history.no_activity", lang)
                await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
                return

            # Create guild-wide stats embed
            embed = create_embed(t("commands.chat_history.server_title", lang, guild=interaction.guild.name), discord.Color.blue())

            embed.description = t("commands.chat_history.longterm_header", lang)
            embed.description += t("commands.chat_history.total_messages", lang, count=stats['total_365d'])
            embed.description += t("commands.chat_history.avg_day", lang, avg=stats['avg_per_day'])

            if stats['busiest_day']:
                busiest_str = format_timestamp(stats['busiest_day']['date'], 'D', guild_id, self.db, lang)
                embed.description += t("commands.chat_history.busiest_day", lang, count=stats['busiest_day']['count'], date=busiest_str)

            if stats['quietest_day']:
                quietest_str = format_timestamp(stats['quietest_day']['date'], 'D', guild_id, self.db, lang)
                embed.description += t("commands.chat_history.quietest_day", lang, count=stats['quietest_day']['count'], date=quietest_str)

            embed.description += "\n" + t("commands.chat_history.period_header", lang)
            embed.description += t("commands.chat_history.period_30", lang, count=stats['total_30d'])
            embed.description += t("commands.chat_history.period_90", lang, count=stats['total_90d'])
            embed.description += t("commands.chat_history.period_365", lang, count=stats['total_365d'])

            embed.description += t("commands.chat_history.recent_header", lang)
            embed.description += t("commands.chat_history.recent_today", lang, count=stats['today'])
            embed.description += t("commands.chat_history.recent_week", lang, count=stats['total_7d'])
            embed.description += t("commands.chat_history.recent_month", lang, count=stats['total_30d'])

            # Get total member count for comparison
            total_members = len([m for m in interaction.guild.members if not m.bot])

            embed.description += t("commands.chat_history.member_stats_header", lang)
            embed.description += t("commands.chat_history.active_members", lang, active=stats['active_members_30d'], total=total_members)
            if stats['avg_per_member'] > 0:
                embed.description += t("commands.chat_history.per_member", lang, count=stats['avg_per_member'])

            embed.set_footer(text=t("commands.chat_history.footer_guild", lang))
            
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            logger.info(f"User {interaction.user} used /chat-history (guild-wide) in guild {interaction.guild.name}")
            return

        # User-specific stats (existing functionality)
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = await asyncio.to_thread(self.db.find_member_by_name, guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.role_history.not_found", lang), lang),
                ephemeral=not channels_restricted
            )
            return

        user_id = member_data['user_id']
        username = member_data['username'] if member_data['username'] else t("common.unknown", lang)

        # Get 365 days of message activity
        activity_trend = await asyncio.to_thread(self.db.get_message_activity_trend, guild_id, user_id, days=365)

        if not activity_trend:
            embed = create_embed(t("commands.chat_history.user_title", lang, username=username), discord.Color.blue())
            embed.description = t("commands.chat_history.no_activity", lang)
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Calculate statistics
        total_messages = sum(record['message_count'] for record in activity_trend)
        avg_per_day = round(total_messages / 365, 1)
        max_day = max(activity_trend, key=lambda r: r['message_count'])
        min_day = min(activity_trend, key=lambda r: r['message_count'])
        max_day_str = format_timestamp(max_day['date'], 'D', guild_id, self.db, lang)
        min_day_str = format_timestamp(min_day['date'], 'D', guild_id, self.db, lang)

        # Get summary statistics
        activity_stats_30 = await asyncio.to_thread(self.db.get_message_activity_period, guild_id, user_id, days=30)
        activity_stats_90 = await asyncio.to_thread(self.db.get_message_activity_period, guild_id, user_id, days=90)

        # Create main embed with statistics
        embed = create_embed(t("commands.chat_history.user_title", lang, username=username), discord.Color.blue())

        embed.description = t("commands.chat_history.longterm_header", lang)
        embed.description += t("commands.chat_history.total_messages", lang, count=total_messages)
        embed.description += t("commands.chat_history.avg_day_user", lang, avg=avg_per_day)
        embed.description += t("commands.chat_history.busiest_day", lang, count=max_day['message_count'], date=max_day_str)
        embed.description += t("commands.chat_history.quietest_day_user", lang, count=min_day['message_count'], date=min_day_str)

        embed.description += t("commands.chat_history.period_header", lang)
        embed.description += t("commands.chat_history.period_30", lang, count=activity_stats_30['this_month'])
        embed.description += t("commands.chat_history.period_90", lang, count=activity_stats_90['this_month'])
        embed.description += t("commands.chat_history.period_365", lang, count=total_messages)

        # Calculate monthly breakdown for last 90 days
        if activity_trend:
            now = datetime.now()
            current_month_count = sum(r['message_count'] for r in activity_trend
                                     if (now.year == datetime.fromtimestamp(r['date']).year and
                                         now.month == datetime.fromtimestamp(r['date']).month))

            embed.description += t("commands.chat_history.recent_header", lang)
            embed.description += t("commands.chat_history.recent_month_user", lang, count=current_month_count)
            embed.description += t("commands.chat_history.recent_week", lang, count=activity_stats_30['this_week'])
            embed.description += t("commands.chat_history.recent_today", lang, count=activity_stats_30['today'])

        embed.set_footer(text=t("commands.chat_history.footer_user", lang))

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /chat-history for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="mystats", description="📊 View your own activity statistics")
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def mystats(self, interaction: discord.Interaction):
        """
        Display the caller's own tracked statistics.
        Output is always ephemeral - your stats are shown only to you.

        Args:
            interaction: Discord interaction
        """
        # Own stats are always ephemeral, so acknowledge before any DB work —
        # the permission check reads the DB and must not spend the 3s token window
        await interaction.response.defer(ephemeral=True, thinking=True)

        # Check permissions
        can_proceed, error_embed, _, lang = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        guild_id = interaction.guild_id
        user_id = interaction.user.id

        member_data = await asyncio.to_thread(self.db.get_member, guild_id, user_id)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed(t("commands.mystats.not_tracked", lang), lang),
                ephemeral=True
            )
            return

        username = member_data['username'] if member_data['username'] else t("common.unknown", lang)
        embed = create_embed(t("commands.mystats.title", lang, username=username), discord.Color.blue())
        embed.description = ""

        # ===== MEMBERSHIP SECTION =====
        if member_data['join_date']:
            join_str = format_timestamp(member_data['join_date'], 'F', guild_id, self.db, lang)
            join_position = member_data.get('join_position')
            if join_position:
                embed.description += t("commands.whois.joined_with_position", lang, date=join_str, position=join_position)
            else:
                embed.description += t("commands.whois.joined", lang, date=join_str)

        nickname = member_data['nickname'] if member_data['nickname'] else t("common.not_set", lang)
        embed.description += t("commands.whois.nickname", lang, nickname=nickname)

        # Own nickname history - it's the caller's data, so no admin gate
        if member_data.get('nickname_history'):
            try:
                history = json.loads(member_data['nickname_history'])
                previous_nicknames = [n for n in history if n != nickname]
                if previous_nicknames:
                    embed.description += t("commands.whois.previously_known", lang, names=', '.join(previous_nicknames))
            except (json.JSONDecodeError, TypeError):
                pass

        embed.description += "\n"

        # ===== STATUS SECTION =====
        member = interaction.guild.get_member(user_id)
        if member and member.status != discord.Status.offline:
            embed.description += t("commands.whois.last_seen_online", lang)
        elif member_data['last_seen'] and member_data['last_seen'] != 0:
            embed.description += t("commands.whois.last_seen", lang, date=format_timestamp(member_data['last_seen'], 'R', guild_id, self.db, lang))
        else:
            embed.description += t("commands.whois.last_seen_unavailable", lang)

        embed.description += "\n"

        # ===== ACTIVITY SECTION =====
        activity_stats = await asyncio.to_thread(
            self.db.get_message_activity_period, guild_id, user_id, 30
        )
        activity_trend = await asyncio.to_thread(
            self.db.get_message_activity_trend, guild_id, user_id, 365
        )

        embed.description += t("commands.mystats.activity_header", lang)
        embed.description += t("commands.whois.activity_today", lang, count=activity_stats['today'])
        embed.description += t("commands.whois.activity_week", lang, count=activity_stats['this_week'])
        embed.description += t("commands.whois.activity_month", lang, count=activity_stats['this_month'])

        if activity_trend:
            total_365 = sum(r['message_count'] for r in activity_trend)
            busiest = max(activity_trend, key=lambda r: r['message_count'])
            busiest_str = format_timestamp(busiest['date'], 'D', guild_id, self.db, lang)
            embed.description += t("commands.mystats.activity_365", lang, count=total_365)
            embed.description += t("commands.mystats.activity_busiest", lang, count=busiest['message_count'], date=busiest_str)

        # Percentile rank vs the rest of the server (last 30 days)
        percentile = await asyncio.to_thread(
            self.db.get_activity_percentile, guild_id, user_id, 30
        )
        if percentile:
            if percentile['caller_total'] > 0:
                embed.description += t(
                    "commands.mystats.percentile", lang,
                    pct=percentile['percentile'], rank=percentile['rank'],
                    total=percentile['total_ranked']
                )
            else:
                embed.description += t("commands.mystats.percentile_unranked", lang)

        embed.set_footer(text=t("commands.mystats.footer", lang))

        view = JourneyView(self, guild_id, user_id, username, lang)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} used /mystats in guild {interaction.guild.name}")

    @app_commands.command(name="forgetme", description="🗑️ Delete your tracked data and opt out of tracking")
    @app_commands.checks.cooldown(1, 60.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def forgetme(self, interaction: discord.Interaction):
        """
        Privacy opt-out: deletes the caller's data in all guilds and stops tracking.

        Deliberately skips the user-role permission gate — a server cannot block
        someone from a privacy action. Idempotent: running it while already
        opted out simply retries the purge (deleting nothing if nothing exists).

        Args:
            interaction: Discord interaction
        """
        lang = guild_language(await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id))
        embed = create_embed(t("commands.forgetme.confirm_title", lang), discord.Color.orange())
        embed.description = t("commands.forgetme.confirm_desc", lang)
        view = ForgetMeConfirmView(self.bot, self.db, interaction.user.id, lang)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="optin", description="✅ Re-enable activity tracking for your account")
    @app_commands.checks.cooldown(1, 60.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def optin(self, interaction: discord.Interaction):
        """
        Re-enable tracking after a /forgetme opt-out. Tracking starts fresh —
        previously deleted data is not restored.

        Args:
            interaction: Discord interaction
        """
        user_id = interaction.user.id
        lang = guild_language(await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id))

        if user_id not in self.bot.opted_out_users:
            await interaction.response.send_message(
                embed=create_error_embed(t("commands.optin.not_opted_out", lang), lang),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        if not await asyncio.to_thread(self.db.remove_opted_out_user, user_id):
            await interaction.followup.send(
                embed=create_error_embed(t("commands.optin.update_failed", lang), lang),
                ephemeral=True
            )
            return

        self.bot.opted_out_users.discard(user_id)

        # Re-create the member record in every mutual guild right away.
        # Message tracking only counts members that already exist in the
        # database, so without this the user would stay untracked until
        # their next presence or profile update.
        tracking_cog = self.bot.get_cog('TrackingCog')
        if tracking_cog:
            for guild in self.bot.guilds:
                member = guild.get_member(user_id)
                if member:
                    await asyncio.to_thread(tracking_cog._ensure_member_exists, member)

        await interaction.followup.send(
            embed=create_success_embed(t("commands.optin.success", lang), lang),
            ephemeral=True
        )
        logger.info(f"User {interaction.user} ({user_id}) opted back in to tracking")

    @app_commands.command(name="about", description="ℹ️ About this bot")
    async def about(self, interaction: discord.Interaction):
        import psutil
        import os
        import sys

        # Stats gathering can exceed the 3s interaction window on a cold cache
        await interaction.response.defer(ephemeral=True)

        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id) if interaction.guild_id else None
        lang = guild_language(guild_config)
        embed = create_embed(t("commands.about.title", lang), discord.Color.green())

        embed.description = t("commands.about.description", lang)

        # Bot Statistics
        bot_stats = await asyncio.to_thread(self.db.get_bot_statistics)

        embed.add_field(
            name=t("commands.about.servers_served", lang),
            value=f"{bot_stats['total_guilds']:,}",
            inline=True
        )

        embed.add_field(
            name=t("commands.about.users_counted", lang),
            value=f"{bot_stats['total_users']:,}",
            inline=True
        )
        
        # Add empty field for layout (3 columns)
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        # Message Activity
        embed.add_field(
            name=t("commands.about.messages_24h", lang),
            value=f"{bot_stats['last_24h']:,}",
            inline=True
        )

        embed.add_field(
            name=t("commands.about.messages_7d", lang),
            value=f"{bot_stats['last_7d']:,}",
            inline=True
        )

        # Add empty field to keep the second row aligned with the first
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        # System Resources
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        # cpu_percent sleeps for the full interval, so keep it off the event loop
        cpu_percent = await asyncio.to_thread(process.cpu_percent, 0.1)
        thread_count = process.num_threads()
        
        # Bot uptime
        if self.bot.start_time:
            uptime_delta = datetime.now(timezone.utc) - self.bot.start_time
            days = uptime_delta.days
            hours, remainder = divmod(uptime_delta.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            if days > 0:
                uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
            elif hours > 0:
                uptime_str = f"{hours}h {minutes}m {seconds}s"
            else:
                uptime_str = f"{minutes}m {seconds}s"
        else:
            uptime_str = "Unknown"
        
        # Latency
        latency_ms = round(self.bot.latency * 1000)
        
        # Database status
        db_health = await asyncio.to_thread(self.db.get_database_health)
        db_status = "✅ Healthy" if db_health['status'] == 'healthy' else "❌ Unhealthy"
        
        # Python version
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        
        system_info = (
            f"```\n"
            f"Memory:    {memory_mb:.1f} MB\n"
            f"CPU:       {cpu_percent:.1f}%\n"
            f"Threads:   {thread_count}\n"
            f"Uptime:    {uptime_str}\n"
            f"Latency:   {latency_ms} ms\n"
            f"Python:    {python_version}\n"
            f"Database:  {db_status}\n"
            f"DB Size:   {db_health['file_size_mb']:.2f} MB\n"
            f"```"
        )
        
        embed.add_field(
            name=t("commands.about.system_resources", lang),
            value=system_info,
            inline=False
        )

        embed.add_field(
            name=t("commands.about.privacy_title", lang),
            value=t("commands.about.privacy_value", lang),
            inline=False
        )

        embed.add_field(
            name=t("commands.about.help_title", lang),
            value=t("commands.about.help_value", lang),
            inline=False
        )

        embed.set_footer(text=t("commands.about.footer", lang))

        await interaction.followup.send(embed=embed)

    @app_commands.command(name="search", description="🔍 Search and filter server members")
    @app_commands.guild_only()
    @app_commands.describe(
        roles="Filter by roles (comma-separated, e.g., @Mod,@Admin)",
        status="Filter by presence: online, offline, idle, dnd, or all",
        inactive="Days since last seen - use >30 (more than), <7 (less than), or =14 (exactly)",
        activity="Messages in last 30 days - examples: >100, <10, =50",
        joined="Join date filter - format: >2024-01-01, <2023-06-01, =2025-01-15",
        departed="Left-date filter (lists members who left) - format: >2024-01-01, <2023-06-01, =2025-01-15",
        username="Search username (partial match, case-insensitive)",
        export="Export results as file: csv, txt, or none"
    )
    async def search(
        self,
        interaction: discord.Interaction,
        roles: str = None,
        status: str = None,
        inactive: str = None,
        activity: str = None,
        joined: str = None,
        departed: str = None,
        username: str = None,
        export: str = "none"
    ):
        """Advanced member search with filtering and export."""
        # Check admin permission
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        lang = guild_language(guild_config)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                t("commands.search.admin_required", lang),
                ephemeral=True
            )
            return

        # Validate export parameter
        if export and export.lower() not in ['none', 'csv', 'txt']:
            await interaction.response.send_message(
                t("commands.search.invalid_export", lang, export=export),
                ephemeral=True
            )
            return

        # Check channel restrictions
        allowed_channels_json = guild_config.get('allowed_channels') if guild_config else None
        channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False

        # Check if command is allowed in current channel
        if not is_channel_allowed(interaction.channel_id, guild_config):
            await interaction.response.send_message(
                t("commands.search.channel_not_allowed", lang),
                ephemeral=True
            )
            return

        # Defer response since this might take a while
        await interaction.response.defer(ephemeral=not channels_restricted)

        guild = interaction.guild
        if not guild:
            await interaction.followup.send(t("commands.search.guild_only", lang), ephemeral=True)
            return

        guild_id = guild.id

        try:
            # Parse all filter parameters
            filters = self._parse_search_filters(
                roles=roles,
                status=status,
                inactive=inactive,
                activity=activity,
                joined=joined,
                departed=departed,
                username=username,
                guild=guild,
                lang=lang
            )
        except ValueError as e:
            await interaction.followup.send(t("commands.search.invalid_filter", lang, error=e), ephemeral=True)
            return

        # Get all members from database. A departed filter targets members who
        # have left, so those rows must be included in the fetch.
        db_members = await asyncio.to_thread(self.db.get_guild_members, guild_id, include_left='departed' in filters)

        # Restrict to the guild's track_only_roles filter (applied at read time;
        # all members are stored regardless of the filter).
        tracked = await asyncio.to_thread(self.db.get_tracked_user_ids, guild_id)
        if tracked is not None:
            tracked_set = set(tracked)
            db_members = [m for m in db_members if m['user_id'] in tracked_set]

        # Get Discord members from cache (chunked in background after on_ready,
        # so this may be partial shortly after startup — misses are tolerated below)
        discord_members = {m.id: m for m in guild.members}

        # Apply filters. Enrichment and the activity filter hit the DB per
        # member, so the whole loop runs off the event loop in one batch.
        def _apply_filters() -> tuple[list, int]:
            results = []
            misses = 0

            for member_data in db_members:
                discord_member = discord_members.get(member_data['user_id'])

                if not discord_member:
                    misses += 1
                    # Member not in cache (left server or cache incomplete)
                    if filters.get('status') or filters.get('roles'):
                        # Skip - these filters require Discord data
                        continue
                    # Database-only filters still work
                    if self._matches_db_filters(member_data, filters):
                        results.append(self._create_db_only_result(member_data, lang))
                else:
                    # Full Discord data available
                    if self._matches_all_filters(member_data, discord_member, filters):
                        results.append(self._enrich_member_data(member_data, discord_member, lang))

            return results, misses

        filtered, cache_misses = await asyncio.to_thread(_apply_filters)

        # Log cache misses. A departed search always misses (left members are
        # never in the Discord cache), so the warning is only noise there.
        if cache_misses > 0 and 'departed' not in filters:
            logger.warning(f"Search had {cache_misses} cache misses in guild {guild_id}")

        # Check if no results
        if len(filtered) == 0:
            await interaction.followup.send(
                t("commands.search.no_results", lang),
                ephemeral=not channels_restricted
            )
            return

        # Apply result limit
        MAX_RESULTS = 1000
        if len(filtered) > MAX_RESULTS:
            await interaction.followup.send(
                t("commands.search.too_many", lang, count=len(filtered), max=MAX_RESULTS),
                ephemeral=not channels_restricted
            )
            filtered = filtered[:MAX_RESULTS]

        # Sort by last_seen (most recent first)
        filtered.sort(key=lambda x: x.get('last_seen_ts', 0), reverse=True)

        # Handle export or display
        if export.lower() in ["csv", "txt"]:
            await self._export_search_results(interaction, filtered, export.lower(), filters, channels_restricted, lang)
        else:
            await self._display_search_results(interaction, filtered, filters, channels_restricted, lang)

    @app_commands.command(name="user-stats", description="📊 View server statistics and analytics (Admin only)")
    @app_commands.guild_only()
    async def user_stats(self, interaction: discord.Interaction):
        """
        Display comprehensive server statistics with interactive dashboard.
        Shows overview with buttons to access detailed reports.
        
        Args:
            interaction: Discord interaction
        """
        # Admin-only: server-wide analytics is a server-management action
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        lang = guild_language(guild_config)

        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(t("errors.no_permission", lang, role=bot_admin_role_name), lang),
                ephemeral=True
            )
            return

        # Check channel restrictions
        allowed_channels_json = guild_config.get('allowed_channels') if guild_config else None
        channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False

        # Check if command is allowed in current channel
        if not is_channel_allowed(interaction.channel_id, guild_config):
            await interaction.response.send_message(
                t("commands.search.channel_not_allowed", lang),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=not channels_restricted)

        try:
            # Get overview statistics
            stats = await asyncio.to_thread(self.db.get_server_snapshot_stats, interaction.guild_id)

            if not stats:
                await interaction.followup.send(
                    t("commands.user_stats.stats_failed", lang),
                    ephemeral=not channels_restricted
                )
                return

            # Get previous month stats for comparison
            prev_stats = await asyncio.to_thread(self.db.get_member_growth_stats, interaction.guild_id, days=60)
            growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0

            # Add guild_id to stats for distribution chart
            stats['guild_id'] = interaction.guild_id

            # Create overview embed (helper makes its own DB queries, so off-loop)
            embed = await asyncio.to_thread(self._create_stats_overview_embed, stats, growth_rate, lang)

            # Create interactive view
            view = UserStatsView(interaction.guild_id, self.db, lang)

            await interaction.followup.send(embed=embed, view=view, ephemeral=not channels_restricted)
            logger.info(f"User {interaction.user} viewed user-stats in guild {interaction.guild.name}")

        except Exception as e:
            logger.error(f"Failed to display user stats: {e}", exc_info=True)
            await interaction.followup.send(
                t("commands.user_stats.error", lang, error=e),
                ephemeral=not channels_restricted
            )

    def _create_stats_overview_embed(self, stats: dict, growth_rate: float, lang: str = 'en') -> discord.Embed:
        """Create the main overview embed for user stats."""
        embed = create_embed(t("commands.user_stats.overview_title", lang), discord.Color.blue())

        # Format growth indicator
        growth_indicator = "📈" if growth_rate > 0 else "📉" if growth_rate < 0 else "➡️"
        growth_text = t("commands.user_stats.vs_last_month", lang, indicator=growth_indicator, pct=abs(growth_rate)) if growth_rate != 0 else t("commands.user_stats.no_change", lang)

        # Member counts section
        active_pct = (stats['active_30d'] / stats['total_members'] * 100) if stats['total_members'] > 0 else 0
        inactive_pct = (stats['inactive_30d'] / stats['total_members'] * 100) if stats['total_members'] > 0 else 0

        embed.description = t(
            "commands.user_stats.overview_desc", lang,
            total=stats['total_members'], growth=growth_text,
            active=stats['active_30d'], active_pct=active_pct,
            inactive=stats['inactive_30d'], inactive_pct=inactive_pct
        )

        # This month section
        net_indicator = "🔼" if stats['net_growth'] > 0 else "🔽" if stats['net_growth'] < 0 else "➡️"
        embed.add_field(
            name=t("commands.user_stats.this_month_title", lang),
            value=t(
                "commands.user_stats.this_month_value", lang,
                joins=stats['joins_this_month'], leaves=stats['leaves_this_month'],
                indicator=net_indicator, net=stats['net_growth']
            ),
            inline=False
        )

        # Activity section
        embed.add_field(
            name=t("commands.user_stats.activity_title", lang),
            value=t(
                "commands.user_stats.activity_value", lang,
                total=stats['total_messages_30d'], avg=stats['avg_messages_per_member'],
                user=stats['most_active_user'], count=stats['most_active_count']
            ),
            inline=False
        )
        
        # Last Seen Distribution
        activity_stats = self.db.get_activity_stats(stats.get('guild_id', 0))
        if activity_stats:
            def create_bar(count: int, max_count: int, length: int = 20) -> str:
                if max_count == 0:
                    return "░" * length
                filled = int((count / max_count) * length)
                return "█" * filled + "░" * (length - filled)

            max_offline = max(
                activity_stats['offline_1h'],
                activity_stats['offline_24h'],
                activity_stats['offline_7d'],
                activity_stats['offline_30d'],
                activity_stats['offline_30d_plus'],
                1
            )

            chart = (
                f"```\n"
                f"<1 hour  {create_bar(activity_stats['offline_1h'], max_offline)} {activity_stats['offline_1h']:>5,}\n"
                f"<24 hrs  {create_bar(activity_stats['offline_24h'], max_offline)} {activity_stats['offline_24h']:>5,}\n"
                f"<7 days  {create_bar(activity_stats['offline_7d'], max_offline)} {activity_stats['offline_7d']:>5,}\n"
                f"<30 days {create_bar(activity_stats['offline_30d'], max_offline)} {activity_stats['offline_30d']:>5,}\n"
                f"30+ days {create_bar(activity_stats['offline_30d_plus'], max_offline)} {activity_stats['offline_30d_plus']:>5,}\n"
                f"```"
            )

            embed.add_field(
                name=t("commands.user_stats.distribution_title", lang),
                value=chart,
                inline=False
            )

        # Participation gap: present-but-silent (lurkers) and never-active (ghosts)
        segments = self.db.get_participation_segments(stats.get('guild_id', 0))
        if segments and (segments['lurkers'] or segments['ghosts']):
            embed.add_field(
                name=t("commands.user_stats.participation_title", lang),
                value=t(
                    "commands.user_stats.participation_value", lang,
                    lurkers=segments['lurkers'], pct=round(segments['lurker_pct']),
                    ghosts=segments['ghosts']
                ),
                inline=False
            )

        embed.set_footer(text=t("commands.user_stats.overview_footer", lang))
        return embed

    def _create_journey_embed(self, journey: dict, member, username: str,
                              guild_id: int, lang: str = 'en') -> discord.Embed:
        """Render a member's participation journey (from get_member_journey).

        Milestones and durations built entirely from message-activity rollups
        plus the single last_seen column — no presence history involved.
        """
        embed = create_embed(t("commands.journey.title", lang, username=username), discord.Color.teal())
        embed.description = ""

        if journey['join_date']:
            embed.description += t("commands.journey.joined", lang,
                                   date=format_timestamp(journey['join_date'], 'R', guild_id, self.db, lang))

        if journey['first_active']:
            # "Same day" when the first message lands on the UTC day they joined.
            same_day = (journey['join_date']
                        and journey['first_active'] // 86400 == journey['join_date'] // 86400)
            when = (t("commands.journey.same_day", lang) if same_day
                    else format_timestamp(journey['first_active'], 'R', guild_id, self.db, lang))
            embed.description += t("commands.journey.first_activity", lang, when=when)
            embed.description += t("commands.journey.active_days", lang, days=journey['active_days'])
            embed.description += t("commands.journey.messages", lang, count=journey['total_messages'])
            embed.description += t("commands.journey.longest_streak", lang, days=journey['longest_streak'])
            embed.description += t("commands.journey.longest_silence", lang, days=journey['longest_gap'])
        else:
            embed.description += t("commands.journey.no_activity", lang)

        # Only meaningful once a member has come back from a 30+ day absence.
        if journey['return_count'] > 0 and journey['longest_away_days']:
            embed.description += t("commands.journey.returned", lang, days=journey['longest_away_days'])

        if journey['last_active']:
            embed.description += t("commands.journey.last_message", lang,
                                   when=format_timestamp(journey['last_active'], 'R', guild_id, self.db, lang))

        if member and member.status != discord.Status.offline:
            embed.description += t("commands.journey.last_seen_online", lang)
        elif journey['last_seen']:
            embed.description += t("commands.journey.last_seen", lang,
                                   when=format_timestamp(journey['last_seen'], 'R', guild_id, self.db, lang))

        embed.set_footer(text=t("commands.journey.footer", lang))
        return embed

    def _parse_search_filters(self, roles, status, inactive, activity, joined, departed, username, guild, lang='en') -> dict:
        """Parse and validate all filter parameters."""
        filters = {}

        # Parse roles
        if roles:
            role_list = []
            
            # Detect if input contains Discord role mentions (<@&123>)
            if '<@&' in roles:
                # Discord mention format: split by spaces
                role_strings = roles.split()
            else:
                # Plain text format: split by commas (supports multi-word role names)
                role_strings = [r.strip() for r in roles.split(',')]
            
            logger.info(f"Role filter input: '{roles}' -> parsed as: {role_strings}")
            
            for role_str in role_strings:
                role_str = role_str.strip()
                if not role_str:
                    continue
                    
                # Handle @mention format <@&123456>
                if role_str.startswith('<@&') and role_str.endswith('>'):
                    try:
                        role_id = int(role_str[3:-1])
                        role = guild.get_role(role_id)
                        if role:
                            role_list.append(role.id)
                            logger.info(f"  Found role by mention: {role.name} (ID: {role.id})")
                    except ValueError:
                        # Invalid role ID format, skip
                        logger.warning(f"  Invalid role mention format: {role_str}")
                        continue
                else:
                    # Search by name (case-insensitive)
                    role = discord.utils.find(lambda r: r.name.lower() == role_str.lower(), guild.roles)
                    if role:
                        role_list.append(role.id)
                        logger.info(f"  Found role by name: '{role_str}' -> {role.name} (ID: {role.id})")
                    else:
                        logger.warning(f"  Role not found: '{role_str}'")
            
            if role_list:
                filters['roles'] = role_list
                logger.info(f"Final role filter: {len(role_list)} role(s) - IDs: {role_list}")
            else:
                # User specified role filter but no valid roles found
                # Set to empty list to force no results (instead of None which means "no filter")
                filters['roles'] = []
                logger.warning("No valid roles found in filter - will return 0 results")

        # Parse status
        if status:
            status_lower = status.lower()
            if status_lower not in ['online', 'offline', 'idle', 'dnd', 'all']:
                raise ValueError(t("commands.search.err_status", lang))
            filters['status'] = status_lower if status_lower != 'all' else None

        # Parse inactive (days)
        if inactive:
            filters['inactive'] = self._parse_filter_value(inactive, 'days', lang)

        # Parse activity (message count)
        if activity:
            filters['activity'] = self._parse_filter_value(activity, 'messages', lang)

        # Parse joined date
        if joined:
            filters['joined'] = self._parse_date_filter(joined, lang)

        # Parse departed (left) date
        if departed:
            filters['departed'] = self._parse_date_filter(departed, lang)

        # Parse username
        if username:
            filters['username'] = username.lower()

        return filters

    def _parse_filter_value(self, filter_str: str, unit: str, lang: str = 'en') -> dict:
        """Parse comparison filters like >30, <7, =14."""
        match = re.match(r'^([<>=]?)(\d+)$', filter_str.strip())
        if not match:
            raise ValueError(t("commands.search.err_filter_format", lang, unit=unit))

        operator = match.group(1) or '='
        try:
            value = int(match.group(2))
        except ValueError:
            raise ValueError(t("commands.search.err_filter_nan", lang, unit=unit))

        # Bounds checking to prevent unreasonable values
        if unit == 'days' and (value < 0 or value > 36500):  # ~100 years
            raise ValueError(t("commands.search.err_days_range", lang, value=value))
        elif unit == 'messages' and (value < 0 or value > 10000000):  # 10M messages
            raise ValueError(t("commands.search.err_messages_range", lang, value=value))

        return {'operator': operator, 'value': value}

    def _parse_date_filter(self, date_str: str, lang: str = 'en') -> dict:
        """Parse date filters like >2024-01-01."""
        match = re.match(r'^([<>=]?)(\d{4}-\d{2}-\d{2})$', date_str.strip())
        if not match:
            raise ValueError(t("commands.search.err_date_format", lang))

        operator = match.group(1) or '='
        try:
            date = datetime.strptime(match.group(2), '%Y-%m-%d')
        except ValueError:
            raise ValueError(t("commands.search.err_date_invalid", lang, date=match.group(2)))

        # Validate date is reasonable (Discord launched in 2015)
        if date.year < 2015:
            raise ValueError(t("commands.search.err_date_min", lang))
        if date.year > 2100:
            raise ValueError(t("commands.search.err_date_max", lang))
        
        timestamp = int(date.replace(tzinfo=timezone.utc).timestamp())
        
        return {'operator': operator, 'value': timestamp}

    def _compare(self, actual: float, filter_spec: dict) -> bool:
        """Compare actual value against filter specification."""
        operator = filter_spec['operator']
        expected = filter_spec['value']
        
        if operator == '>':
            return actual > expected
        elif operator == '<':
            return actual < expected
        else:  # '='
            return actual == expected

    def _matches_db_filters(self, member_data: dict, filters: dict) -> bool:
        """Check if member matches database-only filters (no Discord data needed)."""
        # Username filter (searches both username and nickname/display_name)
        if filters.get('username'):
            username_lower = member_data['username'].lower()
            nickname_lower = (member_data.get('nickname') or member_data.get('display_name') or '').lower()
            search_term = filters['username']
            
            # Check if search term appears in either username or nickname
            if search_term not in username_lower and search_term not in nickname_lower:
                return False

        # Inactive filter
        if filters.get('inactive'):
            last_seen = member_data.get('last_seen')
            if last_seen and last_seen > 0:
                # Member has been seen before, calculate days since
                days_inactive = (datetime.now(timezone.utc).timestamp() - last_seen) / 86400
                if not self._compare(days_inactive, filters['inactive']):
                    return False
            else:
                # No last_seen data or last_seen = 0 (currently online/never tracked)
                # Treat as 0 days inactive
                if not self._compare(0, filters['inactive']):
                    return False

        # Joined filter
        if filters.get('joined'):
            # Database uses 'join_date' column, not 'joined_at'
            joined_at = member_data.get('join_date') or member_data.get('joined_at')
            if joined_at:
                if not self._compare(joined_at, filters['joined']):
                    return False
            else:
                # No join date data - exclude from filter
                logger.debug(f"Member {member_data.get('user_id')} has no join_date, excluding from joined filter")
                return False

        # Departed filter (members who have left, matched by the date they left).
        # left_date is only populated for departures recorded since that column
        # was added, so fall back to last_seen - which on_member_remove also sets
        # to the departure time - to match older departures too.
        if filters.get('departed'):
            if member_data.get('is_active', 1) != 0:
                return False  # still a member, not a departure
            departure_ts = member_data.get('left_date') or member_data.get('last_seen')
            if not departure_ts or not self._compare(departure_ts, filters['departed']):
                return False

        return True

    def _matches_all_filters(self, member_data: dict, discord_member: discord.Member, filters: dict) -> bool:
        """Check if member matches all filters (both DB and Discord data)."""
        # First check database filters
        if not self._matches_db_filters(member_data, filters):
            return False

        # Role filter (Discord data)
        if filters.get('roles') is not None:  # Check for None specifically, not just falsy
            member_role_ids = {r.id for r in discord_member.roles}
            # If filter is empty list, no members match
            if not filters['roles']:
                return False
            # Otherwise check if member has any of the specified roles
            if not any(role_id in member_role_ids for role_id in filters['roles']):
                return False

        # Status filter (Discord data)
        if filters.get('status'):
            if str(discord_member.status) != filters['status']:
                return False

        # Activity filter (requires database query)
        if filters.get('activity'):
            try:
                activity_data = self.db.get_message_activity_period(
                    member_data['guild_id'],
                    member_data['user_id'],
                    days=30
                )
                if not self._compare(activity_data.get('total', 0), filters['activity']):
                    return False
            except Exception as e:
                logger.error(f"Failed to get activity data for user {member_data['user_id']}: {e}")
                # Treat as 0 activity on error
                if not self._compare(0, filters['activity']):
                    return False

        return True

    def _create_db_only_result(self, member_data: dict, lang: str = 'en') -> dict:
        """Create result dict from database data only (no Discord data)."""
        last_seen = member_data.get('last_seen')
        return {
            'username': member_data['username'],
            'display_name': member_data.get('display_name', ''),
            'user_id': member_data['user_id'],
            'status': t('common.unknown', lang),
            'last_seen': last_seen,
            'last_seen_ts': last_seen if last_seen is not None else 0,
            'last_seen_str': self._format_relative_time(last_seen, lang),
            'joined_at': member_data.get('join_date'),
            'joined_at_str': self._format_relative_time(member_data.get('join_date'), lang),
            'join_position': member_data.get('join_position', 'N/A'),
            'roles': [],
            'is_tracked': member_data.get('is_tracked', True),
            'activity_30d': 0,
            'activity_7d': 0,
            'activity_today': 0
        }

    def _enrich_member_data(self, member_data: dict, discord_member: discord.Member, lang: str = 'en') -> dict:
        """Enrich database data with Discord member information."""
        last_seen = member_data.get('last_seen')
        
        # If last_seen is NULL, it means never tracked yet - keep as None to show "Never"
        # If user is currently online but DB has an old timestamp, update to 0
        if discord_member.status != discord.Status.offline and last_seen and last_seen > 0:
            # User is online but DB has an old offline timestamp
            # Set to 0 to show "Online now"
            last_seen = 0
        
        # Get activity data with error handling
        try:
            activity_data = self.db.get_message_activity_period(
                member_data['guild_id'],
                member_data['user_id'],
                days=30
            )
        except Exception as e:
            logger.error(f"Failed to get activity data for user {member_data['user_id']}: {e}")
            activity_data = {'total': 0, 'this_week': 0, 'today': 0}
        
        return {
            'username': discord_member.name,
            'display_name': discord_member.display_name,
            'user_id': discord_member.id,
            'status': str(discord_member.status),
            'last_seen': last_seen,
            'last_seen_ts': last_seen if last_seen is not None else 0,
            'last_seen_str': self._format_relative_time(last_seen, lang),
            'joined_at': member_data.get('join_date'),
            'joined_at_str': self._format_relative_time(member_data.get('join_date'), lang),
            'join_position': member_data.get('join_position', 'N/A'),
            'roles': [r.name for r in discord_member.roles if r.name != '@everyone'],
            'is_tracked': member_data.get('is_tracked', True),
            'activity_30d': activity_data.get('total', 0),
            'activity_7d': activity_data.get('this_week', 0),
            'activity_today': activity_data.get('today', 0)
        }

    def _format_relative_time(self, timestamp: int, lang: str = 'en') -> str:
        """Format timestamp as relative time."""
        if timestamp is None:
            return t('common.never', lang)

        if timestamp == 0:
            return t('commands.search.online_now', lang)

        # Validate timestamp is reasonable (not negative, not too far in future)
        if timestamp < 0:
            return t('common.invalid_date', lang)

        try:
            now = datetime.now(timezone.utc)
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return t('common.invalid_date', lang)
        delta = now - dt

        if delta.days > 365:
            return t('commands.search.years_ago', lang, n=delta.days // 365)
        elif delta.days > 30:
            return t('commands.search.months_ago', lang, n=delta.days // 30)
        elif delta.days > 0:
            return t('commands.search.days_ago', lang, n=delta.days)
        elif delta.seconds > 3600:
            return t('commands.search.hours_ago', lang, n=delta.seconds // 3600)
        elif delta.seconds > 60:
            return t('commands.search.minutes_ago', lang, n=delta.seconds // 60)
        else:
            return t('commands.search.just_now', lang)

    async def _display_search_results(self, interaction: discord.Interaction, results: list, filters: dict, channels_restricted: bool = False, lang: str = 'en'):
        """Display search results with pagination."""
        # Create SearchResultsView
        view = SearchResultsView(results, filters, per_page=15, lang=lang)
        embed = view.create_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=not channels_restricted)

    async def _export_search_results(self, interaction: discord.Interaction, results: list, format: str, filters: dict, channels_restricted: bool = False, lang: str = 'en'):
        """Export search results to file."""
        try:
            if format == "csv":
                file = self._generate_csv(results)
            else:  # txt
                file = self._generate_txt(results, filters)

            await interaction.followup.send(
                t("commands.search.export_success", lang, count=len(results), format=format.upper()),
                file=file,
                ephemeral=not channels_restricted
            )
        except Exception as e:
            logger.error(f"Failed to generate export: {e}", exc_info=True)
            await interaction.followup.send(t("commands.search.export_failed", lang, error=e), ephemeral=not channels_restricted)

    def _generate_csv(self, results: list) -> discord.File:
        """Generate CSV export with all member data."""
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'Username', 'Display Name', 'User ID', 'Status',
            'Last Seen', 'Joined At', 'Join Position',
            'Messages (30d)', 'Messages (7d)', 'Messages (Today)',
            'Roles', 'Is Tracked'
        ], quoting=csv.QUOTE_ALL)  # Quote all fields for safety
        
        writer.writeheader()
        for member in results:
            # Sanitize strings to prevent CSV/formula injection
            username = sanitize_csv_value(member['username'])
            display_name = sanitize_csv_value(member.get('display_name', ''))
            roles = sanitize_csv_value(', '.join(str(r) for r in member.get('roles', [])))
            
            writer.writerow({
                'Username': username,
                'Display Name': display_name,
                'User ID': member['user_id'],
                'Status': member.get('status', 'Unknown'),
                'Last Seen': member['last_seen_str'],
                'Joined At': member['joined_at_str'],
                'Join Position': member.get('join_position', 'N/A'),
                'Messages (30d)': member.get('activity_30d', 0),
                'Messages (7d)': member.get('activity_7d', 0),
                'Messages (Today)': member.get('activity_today', 0),
                'Roles': roles,
                'Is Tracked': 'Yes' if member['is_tracked'] else 'No'
            })
        
        output.seek(0)
        filename = f"member_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return discord.File(fp=StringIO(output.getvalue()), filename=filename)

    def _generate_txt(self, results: list, filters: dict) -> discord.File:
        """Generate readable text export."""
        output = StringIO()
        output.write(f"Member Search Results - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
        output.write(f"Total Members: {len(results)}\n")
        
        # Write filter summary
        filter_text = self._format_filter_summary(filters)
        if filter_text:
            output.write(f"\nFilters Applied:\n{filter_text}\n")
        
        output.write("=" * 80 + "\n\n")
        
        for i, member in enumerate(results, 1):
            output.write(f"{i}. {member['username']}")
            if member['display_name'] and member['display_name'] != member['username']:
                output.write(f" ({member['display_name']})")
            output.write("\n")
            output.write(f"   User ID: {member['user_id']}\n")
            output.write(f"   Status: {member.get('status', 'Unknown')}\n")
            output.write(f"   Last Seen: {member['last_seen_str']}\n")
            output.write(f"   Joined: {member['joined_at_str']}\n")
            output.write(f"   Activity: {member.get('activity_30d', 0)} msgs (30d), "
                        f"{member.get('activity_7d', 0)} msgs (7d), "
                        f"{member.get('activity_today', 0)} today\n")
            if member.get('roles'):
                output.write(f"   Roles: {', '.join(member['roles'])}\n")
            output.write("\n")
        
        output.seek(0)
        filename = f"member_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        return discord.File(fp=output, filename=filename)

    def _format_filter_summary(self, filters: dict) -> str:
        """Format filter dictionary into readable summary."""
        lines = []
        if filters.get('roles'):
            lines.append(f"  • Roles: {len(filters['roles'])} role(s)")
        if filters.get('status'):
            lines.append(f"  • Status: {filters['status']}")
        if filters.get('inactive'):
            lines.append(f"  • Inactive: {filters['inactive']['operator']}{filters['inactive']['value']} days")
        if filters.get('activity'):
            lines.append(f"  • Activity: {filters['activity']['operator']}{filters['activity']['value']} messages")
        if filters.get('joined'):
            try:
                date_str = datetime.fromtimestamp(filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(f"  • Joined: {filters['joined']['operator']}{date_str}")
            except (ValueError, OSError, OverflowError):
                lines.append(f"  • Joined: {filters['joined']['operator']}[Invalid Date]")
        if filters.get('departed'):
            try:
                date_str = datetime.fromtimestamp(filters['departed']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(f"  • Departed: {filters['departed']['operator']}{date_str}")
            except (ValueError, OSError, OverflowError):
                lines.append(f"  • Departed: {filters['departed']['operator']}[Invalid Date]")
        if filters.get('username'):
            lines.append(f"  • Username contains: '{filters['username']}'")
        return '\n'.join(lines) if lines else None


class SearchResultsView(discord.ui.View):
    """Interactive pagination view for search results."""

    def __init__(self, results: list, filters: dict, per_page: int = 15, lang: str = 'en'):
        super().__init__(timeout=300)  # 5 minute timeout
        self.results = results if results else []
        self.filters = filters
        self.lang = lang
        self.per_page = max(1, per_page)  # Ensure at least 1 per page
        self.current_page = 0
        self.max_page = max(0, (len(self.results) - 1) // self.per_page) if self.results else 0

        self.prev_button.label = t("commands.search_view.btn_prev", lang)
        self.next_button.label = t("commands.search_view.btn_next", lang)
        self.export_csv_button.label = t("commands.search_view.btn_export_csv", lang)
        self.export_txt_button.label = t("commands.search_view.btn_export_txt", lang)

        # Update button states
        self._update_buttons()

    def _update_buttons(self):
        """Update button enabled/disabled states."""
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page == self.max_page)
        
        # Disable all buttons if only one page
        if self.max_page == 0:
            self.prev_button.disabled = True
            self.next_button.disabled = True

    def create_embed(self) -> discord.Embed:
        """Create embed for current page."""
        lang = self.lang
        # Bounds check to prevent index errors
        if not self.results:
            return discord.Embed(
                title=t("commands.search_view.title", lang),
                description=t("commands.search_view.no_results_display", lang),
                color=discord.Color.blue()
            )

        # Ensure current_page is within bounds
        self.current_page = max(0, min(self.current_page, self.max_page))

        start = self.current_page * self.per_page
        end = min(start + self.per_page, len(self.results))
        page_results = self.results[start:end]

        embed = discord.Embed(
            title=t("commands.search_view.title", lang),
            description=t("commands.search_view.found", lang, count=len(self.results)),
            color=discord.Color.blue()
        )

        # Add filter summary
        filter_text = self._format_filters()
        if filter_text:
            # Discord embed field value limit is 1024 characters
            if len(filter_text) > 1024:
                filter_text = filter_text[:1021] + "..."
            embed.add_field(name=t("commands.search_view.filters_applied", lang), value=filter_text, inline=False)

        # Add results
        result_lines = []
        for i, member in enumerate(page_results, start=start+1):
            status_emoji = {
                'online': '🟢',
                'idle': '🟡',
                'dnd': '🔴',
                'offline': '⚫',
                'Unknown': '⚪'
            }.get(member.get('status', 'Unknown'), '⚪')

            # Sanitize member data to prevent display issues
            username = str(member['username'])[:32]  # Discord max username length
            display_name = str(member.get('display_name', ''))[:32] if member.get('display_name') else None

            line = t("commands.search_view.result_line", lang, i=i, emoji=status_emoji, username=username)
            if display_name and display_name != username:
                line += t("commands.search_view.result_display", lang, display=display_name)
            line += t("commands.search_view.result_lastseen", lang, last_seen=member['last_seen_str'])
            if member.get('activity_30d', 0) > 0:
                line += t("commands.search_view.result_activity", lang, count=member['activity_30d'])
            result_lines.append(line)

        # Join results and check field value limit (1024 characters)
        result_text = '\n\n'.join(result_lines)
        if len(result_text) > 1024:
            suffix = t("commands.search_view.truncated", lang)
            result_text = result_text[:1024 - len(suffix)] + suffix

        embed.add_field(name=t("commands.search_view.members_field", lang), value=result_text, inline=False)

        embed.set_footer(text=t("commands.search_view.footer", lang, page=self.current_page + 1, total=self.max_page + 1))
        return embed

    def _format_filters(self) -> str:
        """Format filters into readable string."""
        lang = self.lang
        lines = []
        if self.filters.get('roles'):
            lines.append(t("commands.search_view.filter_roles", lang, count=len(self.filters['roles'])))
        if self.filters.get('status'):
            lines.append(t("commands.search_view.filter_status", lang, status=self.filters['status']))
        if self.filters.get('inactive'):
            lines.append(t("commands.search_view.filter_inactive", lang, op=self.filters['inactive']['operator'], value=self.filters['inactive']['value']))
        if self.filters.get('activity'):
            lines.append(t("commands.search_view.filter_activity", lang, op=self.filters['activity']['operator'], value=self.filters['activity']['value']))
        if self.filters.get('joined'):
            try:
                date_str = datetime.fromtimestamp(self.filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(t("commands.search_view.filter_joined", lang, op=self.filters['joined']['operator'], date=date_str))
            except (ValueError, OSError, OverflowError):
                lines.append(t("commands.search_view.filter_joined", lang, op=self.filters['joined']['operator'], date=t("commands.search_view.invalid_date_label", lang)))
        if self.filters.get('departed'):
            try:
                date_str = datetime.fromtimestamp(self.filters['departed']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(t("commands.search_view.filter_departed", lang, op=self.filters['departed']['operator'], date=date_str))
            except (ValueError, OSError, OverflowError):
                lines.append(t("commands.search_view.filter_departed", lang, op=self.filters['departed']['operator'], date=t("commands.search_view.invalid_date_label", lang)))
        if self.filters.get('username'):
            lines.append(t("commands.search_view.filter_username", lang, username=self.filters['username']))
        return '\n'.join(lines) if lines else t("commands.search_view.no_filters", lang)

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        self.current_page = max(0, self.current_page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        self.current_page = min(self.max_page, self.current_page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="📄 Export CSV", style=discord.ButtonStyle.green)
    async def export_csv_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export results as CSV."""
        await interaction.response.defer(ephemeral=True)
        try:
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                'Username', 'Display Name', 'User ID', 'Status',
                'Last Seen', 'Joined At', 'Join Position',
                'Messages (30d)', 'Messages (7d)', 'Messages (Today)',
                'Roles', 'Is Tracked'
            ], quoting=csv.QUOTE_ALL)  # Quote all fields for safety
            
            writer.writeheader()
            for member in self.results:
                # Sanitize strings to prevent CSV/formula injection
                username = sanitize_csv_value(member['username'])
                display_name = sanitize_csv_value(member.get('display_name', ''))
                roles = sanitize_csv_value(', '.join(str(r) for r in member.get('roles', [])))
                
                writer.writerow({
                    'Username': username,
                    'Display Name': display_name,
                    'User ID': member['user_id'],
                    'Status': member.get('status', 'Unknown'),
                    'Last Seen': member['last_seen_str'],
                    'Joined At': member['joined_at_str'],
                    'Join Position': member.get('join_position', 'N/A'),
                    'Messages (30d)': member.get('activity_30d', 0),
                    'Messages (7d)': member.get('activity_7d', 0),
                    'Messages (Today)': member.get('activity_today', 0),
                    'Roles': roles,
                    'Is Tracked': 'Yes' if member['is_tracked'] else 'No'
                })
            
            output.seek(0)
            filename = f"member_search_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            file = discord.File(fp=StringIO(output.getvalue()), filename=filename)
            await interaction.followup.send(
                t("commands.search.export_success", self.lang, count=len(self.results), format="CSV"),
                file=file,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(t("commands.search_view.export_failed", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="📝 Export TXT", style=discord.ButtonStyle.green)
    async def export_txt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export results as TXT."""
        await interaction.response.defer(ephemeral=True)
        try:
            output = StringIO()
            output.write(f"Member Search Results - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
            output.write(f"Total Members: {len(self.results)}\n")
            
            # Write filter summary
            filter_lines = []
            if self.filters.get('roles'):
                filter_lines.append(f"  • Roles: {len(self.filters['roles'])} role(s)")
            if self.filters.get('status'):
                filter_lines.append(f"  • Status: {self.filters['status']}")
            if self.filters.get('inactive'):
                filter_lines.append(f"  • Inactive: {self.filters['inactive']['operator']}{self.filters['inactive']['value']} days")
            if self.filters.get('activity'):
                filter_lines.append(f"  • Activity: {self.filters['activity']['operator']}{self.filters['activity']['value']} messages")
            if self.filters.get('joined'):
                date_str = datetime.fromtimestamp(self.filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                filter_lines.append(f"  • Joined: {self.filters['joined']['operator']}{date_str}")
            if self.filters.get('departed'):
                date_str = datetime.fromtimestamp(self.filters['departed']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                filter_lines.append(f"  • Departed: {self.filters['departed']['operator']}{date_str}")
            if self.filters.get('username'):
                filter_lines.append(f"  • Username contains: '{self.filters['username']}'")
            
            if filter_lines:
                output.write(f"\nFilters Applied:\n")
                output.write('\n'.join(filter_lines) + '\n')
            
            output.write("=" * 80 + "\n\n")
            
            for i, member in enumerate(self.results, 1):
                output.write(f"{i}. {member['username']}")
                if member['display_name'] and member['display_name'] != member['username']:
                    output.write(f" ({member['display_name']})")
                output.write("\n")
                output.write(f"   User ID: {member['user_id']}\n")
                output.write(f"   Status: {member.get('status', 'Unknown')}\n")
                output.write(f"   Last Seen: {member['last_seen_str']}\n")
                output.write(f"   Joined: {member['joined_at_str']}\n")
                output.write(f"   Activity: {member.get('activity_30d', 0)} msgs (30d), "
                            f"{member.get('activity_7d', 0)} msgs (7d), "
                            f"{member.get('activity_today', 0)} today\n")
                if member.get('roles'):
                    output.write(f"   Roles: {', '.join(member['roles'])}\n")
                output.write("\n")
            
            output.seek(0)
            filename = f"member_search_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
            file = discord.File(fp=output, filename=filename)
            await interaction.followup.send(
                t("commands.search.export_success", self.lang, count=len(self.results), format="TXT"),
                file=file,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(t("commands.search_view.export_failed", self.lang, error=e), ephemeral=True)


class JourneyView(discord.ui.View):
    """A single 🧬 Journey button attached to /whois and /mystats results.

    Clicking it posts the member's participation journey as an ephemeral
    followup, leaving the original embed intact. Only added when the viewer is
    allowed to see the member's profile (admin, or the member themselves).
    """

    def __init__(self, cog: 'CommandsCog', guild_id: int, user_id: int,
                 username: str, lang: str = 'en'):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.username = username
        self.lang = lang
        self.journey_button.label = t("commands.journey.button", lang)

    @discord.ui.button(label="🧬 Journey", style=discord.ButtonStyle.primary)
    async def journey_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        try:
            journey = await asyncio.to_thread(
                self.cog.db.get_member_journey, self.guild_id, self.user_id
            )
            if not journey:
                await interaction.followup.send(
                    t("commands.journey.no_data", self.lang), ephemeral=True
                )
                return
            member = interaction.guild.get_member(self.user_id)
            embed = self.cog._create_journey_embed(
                journey, member, self.username, self.guild_id, self.lang
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to show member journey: {e}", exc_info=True)
            await interaction.followup.send(
                t("commands.journey.error", self.lang, error=e), ephemeral=True
            )


class UserStatsView(discord.ui.View):
    """Interactive view for user statistics dashboard."""

    def __init__(self, guild_id: int, db: DatabaseManager, lang: str = 'en'):
        super().__init__(timeout=300)  # 5 minute timeout
        self.guild_id = guild_id
        self.db = db
        self.lang = lang
        self.current_view = 'overview'

        self.retention_button.label = t("commands.stats_view.btn_retention", lang)
        self.growth_button.label = t("commands.stats_view.btn_growth", lang)
        self.leaderboard_button.label = t("commands.stats_view.btn_leaderboard", lang)
        self.health_button.label = t("commands.stats_view.btn_health", lang)
        self.heatmap_button.label = t("commands.stats_view.btn_heatmap", lang)
        self.participation_button.label = t("commands.stats_view.btn_participation", lang)
        self.lifecycle_button.label = t("commands.stats_view.btn_lifecycle", lang)
        self.export_button.label = t("commands.stats_view.btn_export", lang)

    def _make_back_view(self) -> discord.ui.View:
        """Build a one-button view that returns to the stats overview.

        Every sub-panel shares this, so the overview-rebuild logic lives in a
        single place instead of being copy-pasted into each button handler.
        """
        view = discord.ui.View(timeout=300)
        back_button = discord.ui.Button(
            label=t("commands.stats_view.btn_back", self.lang),
            style=discord.ButtonStyle.secondary
        )
        back_button.callback = self._return_to_overview
        view.add_item(back_button)
        return view

    async def _return_to_overview(self, interaction: discord.Interaction):
        """Rebuild and show the stats overview (the Back button's callback)."""
        await interaction.response.defer()
        stats = await asyncio.to_thread(self.db.get_server_snapshot_stats, self.guild_id)
        stats['guild_id'] = self.guild_id
        prev_stats = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=60)
        growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0

        cog = interaction.client.get_cog('CommandsCog')
        overview_embed = await asyncio.to_thread(cog._create_stats_overview_embed, stats, growth_rate, self.lang)
        overview_view = UserStatsView(self.guild_id, self.db, self.lang)

        await interaction.edit_original_response(embed=overview_embed, view=overview_view)

    @discord.ui.button(label="📊 Retention Report", style=discord.ButtonStyle.primary, row=0)
    async def retention_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show retention cohort analysis."""
        await interaction.response.defer()
        
        try:
            cohorts = await asyncio.to_thread(self.db.get_retention_cohorts, self.guild_id)
            funnel = await asyncio.to_thread(self.db.get_activation_funnel, self.guild_id)
            lifespan = await asyncio.to_thread(self.db.get_departure_lifespan, self.guild_id)
            embed = self._create_retention_embed(cohorts, funnel, lifespan)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show retention report: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="📈 Server Growth", style=discord.ButtonStyle.primary, row=0)
    async def growth_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show server growth trends."""
        await interaction.response.defer()
        
        try:
            # Get growth data for multiple periods
            growth_30d = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=30)
            growth_90d = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=90)
            growth_365d = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=365)
            
            embed = self._create_growth_embed(growth_30d, growth_90d, growth_365d)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show growth report: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="🏆 Leaderboard", style=discord.ButtonStyle.primary, row=1)
    async def leaderboard_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show activity leaderboard."""
        await interaction.response.defer()
        
        try:
            # Show leaderboard view with period selector
            view = LeaderboardView(self.guild_id, self.db, self.lang)
            embed = await view.create_leaderboard_embed(days=30)
            
            await interaction.edit_original_response(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Failed to show leaderboard: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="💓 Community Pulse", style=discord.ButtonStyle.primary, row=0)
    async def health_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show the community pulse: a directional read plus window-vs-window vitals (no scoring)."""
        await interaction.response.defer()

        try:
            health = await asyncio.to_thread(self.db.get_server_health, self.guild_id)
            history = await asyncio.to_thread(self.db.get_health_history, self.guild_id)
            embed = self._create_health_embed(health, history)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show server health: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="🔥 Activity Heatmap", style=discord.ButtonStyle.primary, row=1)
    async def heatmap_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show activity heatmap."""
        await interaction.response.defer()
        
        try:
            guild_config = await asyncio.to_thread(self.db.get_guild_config, self.guild_id)
            tz_str = guild_config.get('timezone', 'UTC') if guild_config else 'UTC'
            windows = await asyncio.to_thread(self.db.get_server_activity_windows, self.guild_id, 30, tz_str)
            embed = self._create_heatmap_embed(windows, tz_str)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show activity heatmap: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="👥 Participation", style=discord.ButtonStyle.primary, row=1)
    async def participation_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show lurkers (present but silent) and ghosts (never active)."""
        await interaction.response.defer()

        try:
            lurkers = await asyncio.to_thread(self.db.get_lurkers, self.guild_id, window_days=30, limit=15)
            ghosts = await asyncio.to_thread(self.db.get_ghosts, self.guild_id, window_days=30, limit=15)
            segments = await asyncio.to_thread(self.db.get_participation_segments, self.guild_id, window_days=30)
            # Returning members (30+ day comebacks) share this panel — rare, so the
            # section only renders when there's something to show (see the embed helper).
            since_ts = int(datetime.now(timezone.utc).timestamp()) - (30 * 86400)
            returns_count = await asyncio.to_thread(self.db.count_returns, self.guild_id, since_ts)
            returns = await asyncio.to_thread(self.db.get_recent_returns, self.guild_id, since_ts, 5)
            embed = self._create_participation_embed(segments, lurkers, ghosts, returns_count, returns)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show participation report: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="🔄 Lifecycle", style=discord.ButtonStyle.primary, row=1)
    async def lifecycle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show the member activity lifecycle breakdown (behaviour-based stages)."""
        await interaction.response.defer()

        try:
            segments = await asyncio.to_thread(self.db.get_lifecycle_segments, self.guild_id)
            embed = self._create_lifecycle_embed(segments)

            await interaction.edit_original_response(embed=embed, view=self._make_back_view())

        except Exception as e:
            logger.error(f"Failed to show lifecycle report: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)

    @discord.ui.button(label="📋 Export Report", style=discord.ButtonStyle.green, row=1)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export comprehensive stats to text report."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            stats = await asyncio.to_thread(self.db.get_server_snapshot_stats, self.guild_id)
            growth_30d = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=30)
            growth_90d = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=90)
            leaderboard = await asyncio.to_thread(self.db.get_activity_leaderboard, self.guild_id, days=30, limit=25)
            
            # Generate text report
            output = StringIO()
            
            # Header
            output.write("=" * 80 + "\n")
            output.write("SERVER STATISTICS REPORT\n")
            output.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
            output.write("=" * 80 + "\n\n")
            
            # Overview
            output.write("OVERVIEW\n")
            output.write("-" * 80 + "\n")
            output.write(f"Total Members:     {stats['total_members']}\n")
            output.write(f"Active (30d):      {stats['active_30d']}\n")
            output.write(f"Inactive (30d):    {stats['inactive_30d']}\n\n")
            
            # Growth
            output.write("GROWTH (30 DAYS)\n")
            output.write("-" * 80 + "\n")
            output.write(f"Joins:             {growth_30d.get('joins', 0)}\n")
            output.write(f"Leaves:            {growth_30d.get('leaves', 0)}\n")
            output.write(f"Net Growth:        {growth_30d.get('net_growth', 0)}\n")
            output.write(f"Growth Rate:       {growth_30d.get('growth_rate', 0):.2f}%\n\n")
            
            # Activity
            output.write("ACTIVITY (30 DAYS)\n")
            output.write("-" * 80 + "\n")
            output.write(f"Total Messages:    {stats['total_messages_30d']}\n")
            output.write(f"Avg per Member:    {stats['avg_messages_per_member']:.1f}\n\n")
            
            # Leaderboard
            output.write("TOP 25 MOST ACTIVE MEMBERS\n")
            output.write("-" * 80 + "\n")
            output.write(f"{'Rank':<6} {'Username':<30} {'Display Name':<30} {'Messages':<10}\n")
            output.write("-" * 80 + "\n")
            for i, member in enumerate(leaderboard, 1):
                username = member['username'][:28] if len(member['username']) > 28 else member['username']
                display = member['display_name'][:28] if len(member['display_name']) > 28 else member['display_name']
                output.write(f"{i:<6} {username:<30} {display:<30} {member['total_messages']:<10}\n")
            
            output.seek(0)
            filename = f"server_stats_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
            file = discord.File(fp=output, filename=filename)
            
            await interaction.followup.send(
                t("commands.stats_view.export_success", self.lang),
                file=file,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Failed to export stats: {e}", exc_info=True)
            await interaction.followup.send(t("commands.search_view.export_failed", self.lang, error=e), ephemeral=True)

    def _create_retention_embed(self, cohorts: dict, funnel: dict, lifespan: dict) -> discord.Embed:
        """Create retention report embed (join cohorts + activation funnel + departed-member lifespan)."""
        lang = self.lang
        embed = create_embed(t("commands.stats_view.retention_title", lang), discord.Color.purple())

        embed.description = t("commands.stats_view.retention_desc", lang)

        for period, data in cohorts.items():
            if data['total_joined'] > 0:
                period_name = {
                    '30d': t("commands.stats_view.retention_period_30d", lang),
                    '60d': t("commands.stats_view.retention_period_60d", lang),
                    '90d': t("commands.stats_view.retention_period_90d", lang)
                }.get(period, period)

                embed.add_field(
                    name=t("commands.stats_view.period_field", lang, period=period_name),
                    value=t(
                        "commands.stats_view.retention_field_value", lang,
                        joined=data['total_joined'], active=data['still_active'],
                        recently=data['active_recently'], rate=data['retention_rate']
                    ),
                    inline=False
                )

        if not cohorts or all(c['total_joined'] == 0 for c in cohorts.values()):
            embed.description = t("commands.stats_view.retention_no_data", lang)

        # New-member activation funnel: share of recent joiners who posted during
        # each successive era after joining (windowed "still around", not cumulative).
        checkpoints = funnel.get('checkpoints') if funnel else None
        if checkpoints:
            chart_lines = []
            for cp in checkpoints:
                filled = int((cp['rate'] / 100) * 20)
                bar = "█" * filled + "░" * (20 - filled)
                chart_lines.append(f"{cp['label']:<4}{bar} {cp['rate']:>3.0f}%  (n={cp['matured']:,})")

            value = t("commands.stats_view.funnel_intro", lang)
            value += "\n```\n" + "\n".join(chart_lines) + "\n```"
            if funnel.get('largest_dropoff'):
                frm, to = funnel['largest_dropoff']
                value += t("commands.stats_view.funnel_dropoff", lang, from_label=frm, to_label=to)
            embed.add_field(
                name=t("commands.stats_view.funnel_title", lang), value=value, inline=False
            )
        else:
            embed.add_field(
                name=t("commands.stats_view.funnel_title", lang),
                value=t("commands.stats_view.funnel_no_data", lang),
                inline=False
            )

        # Departed-member lifespan: how long members who left actually stayed —
        # the churn side of retention. Labels are fixed (like the last-seen chart).
        if lifespan['sample'] >= 3:
            b = lifespan['buckets']
            labels = ["<1 day", "<7 days", "<30 days", "<90 days", "90+ days"]
            counts = [b['under_1d'], b['under_7d'], b['under_30d'], b['under_90d'], b['over_90d']]
            max_count = max(counts) if any(counts) else 1
            chart_lines = []
            for label, count in zip(labels, counts):
                filled = int((count / max_count) * 20)
                bar = "█" * filled + "░" * (20 - filled)
                chart_lines.append(f"{label:<8} {bar} {count:>4}")

            value = t(
                "commands.stats_view.lifespan_summary", lang,
                sample=lifespan['sample'], median=lifespan['median_days'], avg=lifespan['avg_days']
            )
            value += "\n```\n" + "\n".join(chart_lines) + "\n```"
            value += t("commands.stats_view.lifespan_early_churn", lang, pct=round(lifespan['early_churn_pct']))
            embed.add_field(
                name=t("commands.stats_view.lifespan_title", lang), value=value, inline=False
            )
        else:
            embed.add_field(
                name=t("commands.stats_view.lifespan_title", lang),
                value=t("commands.stats_view.lifespan_no_data", lang),
                inline=False
            )

        return embed

    def _create_growth_embed(self, growth_30d: dict, growth_90d: dict, growth_365d: dict) -> discord.Embed:
        """Create server growth embed."""
        lang = self.lang
        embed = create_embed(t("commands.stats_view.growth_title", lang), discord.Color.green())

        periods = [
            (t("commands.stats_view.growth_period_30", lang), growth_30d),
            (t("commands.stats_view.growth_period_90", lang), growth_90d),
            (t("commands.stats_view.growth_period_365", lang), growth_365d)
        ]

        for period_name, data in periods:
            if data:
                growth_indicator = "📈" if data['growth_rate'] > 0 else "📉" if data['growth_rate'] < 0 else "➡️"

                embed.add_field(
                    name=t("commands.stats_view.period_field", lang, period=period_name),
                    value=t(
                        "commands.stats_view.growth_field_value", lang,
                        joins=data['joins'], leaves=data['leaves'],
                        net=data['net_growth'], indicator=growth_indicator, rate=abs(data['growth_rate'])
                    ),
                    inline=True
                )

        return embed

    def _create_heatmap_embed(self, windows: dict, tz_str: str = 'UTC') -> discord.Embed:
        """Create activity heatmap embed.

        `windows` is a get_server_activity_windows() result: message activity
        bucketed into the guild's local weekday/hour, so every hour shown here
        (and the peak/recommendation below) is in the guild's own clock.
        """
        lang = self.lang
        embed = create_embed(t("commands.stats_view.heatmap_title", lang), discord.Color.orange())

        if not windows or windows.get('total', 0) == 0:
            embed.description = t("commands.stats_view.heatmap_no_data", lang)
            return embed

        by_weekday = windows['by_weekday']
        by_hour = windows['by_hour']

        # Day of week breakdown. Weekday keys are Monday=0..Sunday=6 ints; only
        # the displayed name is localized. The column is padded to the widest
        # localized name so the monospace chart stays aligned in any language.
        max_day_count = max(by_weekday.values()) if by_weekday else 1
        localized_days = {d: weekday_name(d, lang) for d in range(7)}
        name_width = max(len(n) for n in localized_days.values())

        chart_lines = []
        for d in range(7):
            count = by_weekday.get(d, 0)
            bar_length = int((count / max_day_count) * 20) if max_day_count > 0 else 0
            bar = "█" * bar_length + "░" * (20 - bar_length)
            chart_lines.append(f"{localized_days[d]:<{name_width}} {bar} {count:>6,}")

        # Wrap in code block for monospaced alignment
        embed.add_field(
            name=t("commands.stats_view.heatmap_day_title", lang),
            value="```\n" + "\n".join(chart_lines) + "\n```",
            inline=False
        )

        # Peak day
        if windows['best_weekday'] is not None:
            peak_day = windows['best_weekday']
            embed.add_field(
                name=t("commands.stats_view.heatmap_peak_day_title", lang),
                value=t("commands.stats_view.heatmap_peak_value", lang, label=weekday_name(peak_day, lang), count=by_weekday[peak_day]),
                inline=True
            )

        # Hour of day breakdown
        if sum(by_hour.values()) > 0:
            max_hour_count = max(by_hour.values())
            hour_lines = []

            # Group hours into 6-hour blocks for better readability. The hour
            # ranges are fixed; only the displayed label is localized, padded to
            # the widest label so the monospace chart stays aligned in any
            # language (English's widest is "Afternoon" = 9, the old width).
            time_blocks = [
                ("commands.stats_view.timeblock_night", range(0, 6)),
                ("commands.stats_view.timeblock_morning", range(6, 12)),
                ("commands.stats_view.timeblock_afternoon", range(12, 18)),
                ("commands.stats_view.timeblock_evening", range(18, 24))
            ]
            block_labels = [t(key, lang) for key, _ in time_blocks]
            block_width = max(len(label) for label in block_labels)

            for (key, hour_range), label in zip(time_blocks, block_labels):
                block_total = sum(by_hour.get(h, 0) for h in hour_range)
                bar_length = int((block_total / (max_hour_count * 6)) * 15) if max_hour_count > 0 else 0
                bar = "█" * bar_length + "░" * (15 - bar_length)
                hour_lines.append(f"{label:<{block_width}} {bar} {block_total:>5,}")

            # Wrap in code block for monospaced alignment
            embed.add_field(
                name=t("commands.stats_view.heatmap_time_title", lang),
                value="```\n" + "\n".join(hour_lines) + "\n```",
                inline=False
            )

            # Peak hour
            peak_hour = windows['peak_hour']
            time_label = f"{peak_hour:02d}:00-{(peak_hour+1)%24:02d}:00"
            embed.add_field(
                name=t("commands.stats_view.heatmap_peak_hour_title", lang),
                value=t("commands.stats_view.heatmap_peak_value", lang, label=time_label, count=by_hour[peak_hour]),
                inline=True
            )

        # Actionable recommendation. Gated behind a minimum sample so a whole-
        # server suggestion is never drawn off a handful of messages; the
        # busiest and quietest bands are only meaningful with real coverage.
        if windows['total'] >= 50 and windows['active_days'] >= 7 and windows['recommend']:
            rd, rs, re_ = windows['recommend']
            embed.add_field(
                name=t("commands.stats_view.heatmap_recommend_title", lang),
                value=t("commands.stats_view.heatmap_recommend_value", lang,
                        day=weekday_name(rd, lang),
                        start=f"{rs:02d}:00", end=f"{re_ % 24:02d}:00", tz=tz_str),
                inline=False
            )
            if windows['quiet']:
                qd, qs, qe = windows['quiet']
                embed.add_field(
                    name=t("commands.stats_view.heatmap_quiet_title", lang),
                    value=t("commands.stats_view.heatmap_quiet_value", lang,
                            day=weekday_name(qd, lang),
                            start=f"{qs:02d}:00", end=f"{qe % 24:02d}:00"),
                    inline=True
                )

        return embed

    def _create_participation_embed(self, segments: dict, lurkers: list, ghosts: list,
                                    returns_count: int = 0, returns: list = None) -> discord.Embed:
        """Create the participation-gap embed (lurkers, ghosts, and returning members)."""
        lang = self.lang
        embed = create_embed(t("commands.stats_view.participation_title", lang), discord.Color.dark_grey())

        lurker_count = segments.get('lurkers', 0)
        ghost_count = segments.get('ghosts', 0)
        returns = returns or []

        if not lurker_count and not ghost_count and not returns:
            embed.description = t("commands.stats_view.participation_no_data", lang)
            return embed

        embed.description = t("commands.stats_view.participation_desc", lang)

        def display_name(m: dict) -> str:
            return m['nickname'] if m.get('nickname') else m['username']

        # Lurkers: present but silent — show when each was last seen.
        lurker_lines = []
        for m in lurkers:
            if m['last_seen'] == 0:
                when = t("commands.stats_view.participation_online_now", lang)
            elif m['last_seen']:
                when = t("commands.stats_view.participation_last_seen", lang,
                         when=format_timestamp(m['last_seen'], 'R', self.guild_id, self.db, lang))
            else:
                when = t("commands.stats_view.participation_never_seen", lang)
            lurker_lines.append(t("commands.stats_view.participation_line", lang, name=display_name(m), detail=when))
        if lurker_count > len(lurkers):
            lurker_lines.append(t("commands.stats_view.participation_more", lang, count=lurker_count - len(lurkers)))
        embed.add_field(
            name=t("commands.stats_view.participation_lurkers_title", lang, count=lurker_count),
            value="".join(lurker_lines) if lurker_lines else t("commands.stats_view.participation_none", lang),
            inline=False
        )

        # Ghosts: never active — show when (if ever) they were last seen.
        ghost_lines = []
        for m in ghosts:
            if m['last_seen']:
                when = t("commands.stats_view.participation_last_seen", lang,
                         when=format_timestamp(m['last_seen'], 'R', self.guild_id, self.db, lang))
            else:
                when = t("commands.stats_view.participation_never_seen", lang)
            ghost_lines.append(t("commands.stats_view.participation_line", lang, name=display_name(m), detail=when))
        if ghost_count > len(ghosts):
            ghost_lines.append(t("commands.stats_view.participation_more", lang, count=ghost_count - len(ghosts)))
        embed.add_field(
            name=t("commands.stats_view.participation_ghosts_title", lang, count=ghost_count),
            value="".join(ghost_lines) if ghost_lines else t("commands.stats_view.participation_none", lang),
            inline=False
        )

        # Returning members — only rendered when someone actually came back from a
        # 30+ day absence, so the common (empty) case adds nothing to the panel.
        if returns:
            return_lines = []
            for r in returns:
                display = r['nickname'] if r.get('nickname') else r['username']
                days = r['away_seconds'] // 86400
                returned = format_timestamp(r['returned_at'], 'R', self.guild_id, self.db, lang)
                return_lines.append(t("commands.stats_view.returns_line", lang, name=display, days=days, returned=returned))
            if returns_count > len(returns):
                return_lines.append(t("commands.stats_view.returns_more", lang, count=returns_count - len(returns)))
            embed.add_field(
                name=t("commands.stats_view.returns_title", lang),
                value="".join(return_lines),
                inline=False
            )

        return embed

    def _create_lifecycle_embed(self, segments: dict) -> discord.Embed:
        """Create the member-lifecycle embed: behaviour-based activity stages."""
        lang = self.lang
        embed = create_embed(t("commands.stats_view.lifecycle_title", lang), discord.Color.blurple())

        total = sum(segments.values())
        if total == 0:
            embed.description = t("commands.stats_view.lifecycle_no_data", lang)
            return embed

        # Fixed display order (join → active → fading, with Returned last),
        # independent of the counts so the panel reads the same every time.
        order = ['new', 'exploring', 'active', 'quiet', 'dormant', 'returned']
        lines = []
        for bucket in order:
            label = t(f"commands.stats_view.lifecycle_{bucket}", lang)
            lines.append(t("commands.stats_view.lifecycle_line", lang, label=label, count=segments.get(bucket, 0)))
        embed.description = t("commands.stats_view.lifecycle_desc", lang) + "\n\n" + "\n".join(lines)
        embed.set_footer(text=t("commands.stats_view.lifecycle_footer", lang, total=total))
        return embed

    def _create_health_embed(self, health: dict, history: list = None) -> discord.Embed:
        """Create the server-health embed: current vs previous window for each
        vital, deltas only rendered once the prior window is fully covered."""
        lang = self.lang
        embed = create_embed(t("commands.stats_view.health_title", lang), discord.Color.teal())

        if not health:
            embed.description = t("commands.stats_view.health_no_data", lang)
            return embed

        # Directional headline (no score): posters + messages vs last month,
        # blended into a single up/steady/down read of the community's pulse.
        pulse = compute_community_pulse(health)
        state_label = t(f"commands.stats_view.pulse_{pulse['state']}", lang)
        headline = t("commands.stats_view.pulse_headline", lang, state=state_label)
        if pulse['state'] == 'collecting':
            headline += "\n" + t("commands.stats_view.pulse_collecting_note", lang,
                                 need=60, days=health['days_of_data'])
        elif pulse['posters_delta'] is not None and pulse['messages_delta'] is not None:
            headline += "\n" + t("commands.stats_view.pulse_detail", lang,
                                 posters=format_pct_arrow(pulse['posters_delta']),
                                 messages=format_pct_arrow(pulse['messages_delta']))

        embed.description = headline + "\n\n" + t("commands.stats_view.health_desc", lang)
        weekly_ok = health['weekly_comparable']
        monthly_ok = health['monthly_comparable']

        def with_delta(cur: int, prev: int, comparable: bool) -> str:
            return format_health_delta(cur, prev, lang) if comparable else ""

        # This week vs last week (message-derived, complete UTC days)
        week_lines = [
            t("commands.stats_view.health_posters_line", lang,
              cur=health['posters_7d'], delta=with_delta(health['posters_7d'], health['posters_prev_7d'], weekly_ok)),
            t("commands.stats_view.health_messages_line", lang,
              cur=health['messages_7d'], delta=with_delta(health['messages_7d'], health['messages_prev_7d'], weekly_ok)),
        ]
        if not weekly_ok:
            week_lines.append(t("commands.stats_view.health_no_compare", lang,
                                need=14, days=health['days_of_data']))
        embed.add_field(
            name=t("commands.stats_view.health_week_title", lang),
            value="\n".join(week_lines),
            inline=False
        )

        # Membership flow (joins/leaves/net) intentionally lives only in the
        # Server Growth panel now — it owns membership churn across 30/90/365d,
        # so Community Pulse stays focused on engagement momentum.

        # Participation breadth: share of tracked members who posted
        members = health['active_members']
        breadth_pct = (health['posters_30d'] / members * 100) if members > 0 else 0
        embed.add_field(
            name=t("commands.stats_view.health_breadth_title", lang),
            value=t("commands.stats_view.health_breadth_line", lang,
                    cur=health['posters_30d'], members=members, pct=breadth_pct,
                    delta=with_delta(health['posters_30d'], health['posters_prev_30d'], monthly_ok)),
            inline=False
        )

        # New-member activation (first-week posting rate) intentionally lives
        # only in the Retention panel now — its activation funnel covers the same
        # signal in more detail, so Community Pulse no longer repeats it here.

        # Returning members — 30+ day comebacks, window vs window
        embed.add_field(
            name=t("commands.stats_view.health_returns_title", lang),
            value=t("commands.stats_view.health_returns_line", lang,
                    cur=health['returns_30d'],
                    delta=with_delta(health['returns_30d'], health['returns_prev_30d'], monthly_ok)),
            inline=False
        )

        # Long-range trend from the nightly snapshots: weekly posters as bars,
        # member count alongside. Needs at least two sampled weeks to be a
        # trend; before that the section explains it's still collecting.
        if history and len(history) >= 2:
            max_posters = max(row['posters_7d'] for row in history) or 1
            chart_lines = []
            for row in history:
                label = datetime.fromtimestamp(row['date'], tz=timezone.utc).strftime('%m-%d')
                filled = int((row['posters_7d'] / max_posters) * 12)
                bar = "█" * filled + "░" * (12 - filled)
                chart_lines.append(f"{label} {bar} {row['posters_7d']:>5,} │ {row['active_members']:>6,}")
            embed.add_field(
                name=t("commands.stats_view.health_trend_title", lang),
                value="```\n" + "\n".join(chart_lines) + "\n```",
                inline=False
            )
        else:
            embed.add_field(
                name=t("commands.stats_view.health_trend_title", lang),
                value=t("commands.stats_view.health_trend_no_data", lang),
                inline=False
            )

        embed.set_footer(text=t("commands.stats_view.health_footer", lang))
        return embed


class LeaderboardView(discord.ui.View):
    """Interactive leaderboard view with period selection."""

    def __init__(self, guild_id: int, db: DatabaseManager, lang: str = 'en'):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.db = db
        self.lang = lang
        self.current_period = 30

        self.back_button.label = t("commands.stats_view.btn_back", lang)
        self.period_select.placeholder = t("commands.leaderboard.select_placeholder", lang)
        option_keys = {
            "7": "commands.leaderboard.period_7",
            "30": "commands.leaderboard.period_30",
            "90": "commands.leaderboard.period_90",
            "0": "commands.leaderboard.period_all",
        }
        for option in self.period_select.options:
            if option.value in option_keys:
                option.label = t(option_keys[option.value], lang)

    def _period_name(self, days: int) -> str:
        """Localized name for a leaderboard period."""
        keys = {
            7: "commands.leaderboard.period_7",
            30: "commands.leaderboard.period_30",
            90: "commands.leaderboard.period_90",
            0: "commands.leaderboard.period_all",
        }
        if days in keys:
            return t(keys[days], self.lang)
        return t("commands.leaderboard.period_other", self.lang, days=days)

    async def create_leaderboard_embed(self, days: int) -> discord.Embed:
        """Create leaderboard embed for specified period."""
        lang = self.lang
        self.current_period = days

        period_name = self._period_name(days)

        embed = create_embed(t("commands.leaderboard.title", lang, period=period_name), discord.Color.gold())

        leaderboard = await asyncio.to_thread(self.db.get_activity_leaderboard, self.guild_id, days=days, limit=10)

        if not leaderboard:
            embed.description = t("commands.leaderboard.no_data", lang)
            return embed

        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, member in enumerate(leaderboard, 1):
            medal = medals[i-1] if i <= 3 else t("commands.leaderboard.rank", lang, i=i)
            lines.append(
                t("commands.leaderboard.line", lang, medal=medal, name=member['display_name'], count=member['total_messages'])
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text=t("commands.leaderboard.footer", lang))

        return embed

    @discord.ui.select(
        placeholder="Select time period",
        options=[
            discord.SelectOption(label="Last 7 Days", value="7", emoji="📅"),
            discord.SelectOption(label="Last 30 Days", value="30", emoji="📅", default=True),
            discord.SelectOption(label="Last 90 Days", value="90", emoji="📅"),
            discord.SelectOption(label="All Time", value="0", emoji="🌟")
        ],
        row=0
    )
    async def period_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        """Handle period selection."""
        await interaction.response.defer()
        
        days = int(select.values[0])
        embed = await self.create_leaderboard_embed(days)
        
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="◀️ Back to Overview", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Return to main overview."""
        await interaction.response.defer()
        
        try:
            stats = await asyncio.to_thread(self.db.get_server_snapshot_stats, self.guild_id)
            stats['guild_id'] = self.guild_id
            prev_stats = await asyncio.to_thread(self.db.get_member_growth_stats, self.guild_id, days=60)
            growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0

            cog = interaction.client.get_cog('CommandsCog')
            overview_embed = await asyncio.to_thread(cog._create_stats_overview_embed, stats, growth_rate, self.lang)
            overview_view = UserStatsView(self.guild_id, self.db, self.lang)
            
            await interaction.edit_original_response(embed=overview_embed, view=overview_view)
            
        except Exception as e:
            logger.error(f"Failed to return to overview: {e}", exc_info=True)
            await interaction.followup.send(t("commands.stats_view.error", self.lang, error=e), ephemeral=True)


async def setup(bot: commands.Bot):
    """
    Setup function for loading the cog.

    Args:
        bot: Discord bot instance
    """
    db = bot.db
    config = bot.config
    await bot.add_cog(CommandsCog(bot, db, config))
    logger.info("CommandsCog loaded")

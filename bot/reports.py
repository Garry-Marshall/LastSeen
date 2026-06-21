"""Scheduled report generation for LastSeen bot."""

import discord
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict
from database import DatabaseManager
from bot.utils import create_embed
from bot.locale import t, guild_language, weekday_name

logger = logging.getLogger(__name__)

# Rate limiting for report sending
_last_report_send: Dict[int, float] = {}  # guild_id -> timestamp
_report_locks: Dict[int, asyncio.Lock] = {}  # guild_id -> lock
_RATE_LIMIT_WINDOW = 60  # seconds between reports per guild


def _get_report_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in _report_locks:
        _report_locks[guild_id] = asyncio.Lock()
    return _report_locks[guild_id]


def purge_guild_state(guild_id: int) -> None:
    """Remove all module-level per-guild state for a guild that has been removed."""
    _report_locks.pop(guild_id, None)
    _last_report_send.pop(guild_id, None)


async def generate_activity_report(guild: discord.Guild, db: DatabaseManager, days: int, report_types: list) -> discord.Embed:
    """
    Generate activity summary report.

    Args:
        guild: Discord guild
        db: Database manager
        days: Period to report (7 for weekly, 30 for monthly)
        report_types: Enabled report types; controls the Member Changes section

    Returns:
        Discord embed with activity summary
    """
    guild_id = int(guild.id)
    lang = guild_language(db.get_guild_config(guild_id))

    # Get guild-wide message activity stats
    activity_stats = db.get_guild_message_activity_stats(guild_id, days)
    
    # Get top active users
    top_users = db.get_top_active_users_period(guild_id, days, limit=5)
    
    # Get daily activity for peak day
    daily_activity = db.get_activity_by_day(guild_id, days)
    # Validate daily_activity is not None and not empty before calling max()
    if daily_activity and isinstance(daily_activity, dict) and len(daily_activity) > 0:
        peak_day = max(daily_activity.items(), key=lambda x: x[1])
    else:
        peak_day = (None, 0)
    
    # Create embed
    title_key = 'report.title.weekly' if days == 7 else 'report.title.monthly'
    embed = create_embed(t(title_key, lang, guild=guild.name), discord.Color.blue())
    embed.timestamp = datetime.now(timezone.utc)
    
    # Overall statistics - use appropriate key based on period
    if days <= 7:
        total_messages = activity_stats.get('total_7d', 0)
    elif days <= 30:
        total_messages = activity_stats.get('total_30d', 0)
    elif days <= 90:
        total_messages = activity_stats.get('total_90d', 0)
    else:
        total_messages = activity_stats.get('total_365d', 0)
    avg_per_day = activity_stats.get('avg_per_day', 0)
    
    embed.description = t('report.activity_header', lang, days=days)
    embed.description += t('report.total_messages', lang, total=total_messages)
    embed.description += t('report.daily_average', lang, avg=avg_per_day)

    if peak_day[0]:
        # peak_day[0] is a day name (e.g., 'Monday'), not a timestamp
        embed.description += t('report.peak_day', lang, day=weekday_name(peak_day[0], lang), count=peak_day[1])

    embed.description += "\n"

    # Member changes - only show the counts whose report type is enabled
    show_joined = 'members' in report_types
    show_left = 'departures' in report_types
    if show_joined or show_left:
        embed.description += t('report.member_changes_header', lang)
        joined = left = None
        if show_joined:
            joined = len(db.get_new_members_period(guild_id, days))
            embed.description += t('report.joined', lang, count=joined)
        if show_left:
            left = len(db.get_departed_members_period(guild_id, days))
            embed.description += t('report.left', lang, count=left)
        if show_joined and show_left:
            embed.description += t('report.net', lang, count=joined - left)
        embed.description += "\n"

    # Top contributors
    if top_users:
        embed.description += t('report.top_contributors_header', lang)
        for i, user in enumerate(top_users, 1):
            username = user['username'] or t('common.unknown', lang)
            nickname = user['nickname']
            display = f"{nickname} ({username})" if nickname else username
            embed.description += t('report.contributor_line', lang, rank=i, display=display, count=user['total_messages'])
    else:
        embed.description += t('report.top_contributors_header', lang) + t('report.no_activity', lang)

    return embed


async def send_scheduled_report(guild: discord.Guild, channel_id: int, db: DatabaseManager, 
                                report_types: list, days: int, max_retries: int = 3) -> bool:
    """
    Send scheduled report to the specified channel with rate limiting and retry logic.
    
    Args:
        guild: Discord guild
        channel_id: Channel ID to send report to
        db: Database manager
        report_types: List of report types to include
        days: Period to report (7 for weekly, 30 for monthly)
        max_retries: Maximum number of retry attempts on failure
        
    Returns:
        True if report sent successfully, False otherwise
    """
    async with _get_report_lock(guild.id):
        now = datetime.now(timezone.utc).timestamp()
        last_send = _last_report_send.get(guild.id, 0)

        if now - last_send < _RATE_LIMIT_WINDOW:
            wait_time = _RATE_LIMIT_WINDOW - (now - last_send)
            logger.warning(f"Rate limit: waiting {wait_time:.1f}s before sending report to {guild.name}")
            await asyncio.sleep(wait_time)

        for attempt in range(max_retries):
            try:
                channel = guild.get_channel(channel_id)
                if not channel or not isinstance(channel, discord.TextChannel):
                    logger.error(f"Report channel {channel_id} not found or not a text channel in guild {guild.name}")
                    return False

                # The activity report is the only embed; member join/leave counts
                # are folded into it, so there is no report content without it.
                if 'activity' not in report_types:
                    logger.info(f"No report content to send for guild {guild.name}")
                    return True

                embed = await generate_activity_report(guild, db, days, report_types)
                await channel.send(embed=embed)
                _last_report_send[guild.id] = datetime.now(timezone.utc).timestamp()
                logger.info(f"Sent scheduled report to {channel.name} in guild {guild.name}")
                return True

            except discord.Forbidden:
                logger.error(f"Missing permissions to send report in channel {channel_id} in guild {guild.name}")
                return False

            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited by Discord
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
                    logger.warning(f"Discord rate limit hit, retrying after {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    logger.error(f"Discord HTTP error sending report to {guild.name}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return False

            except Exception as e:
                logger.error(f"Failed to send scheduled report for guild {guild.name} (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return False

        logger.error(f"Failed to send report to {guild.name} after {max_retries} attempts")
        return False

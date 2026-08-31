"""Scheduled report generation for LastSeen bot."""

import discord
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict
from database import DatabaseManager
from bot.utils import create_embed, format_health_delta
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


def generate_activity_report(guild: discord.Guild, db: DatabaseManager, days: int, report_types: list) -> discord.Embed:
    """
    Generate activity summary report.

    Synchronous: every step is a blocking DB read plus in-memory embed building,
    with no awaits. Run it off the event loop (asyncio.to_thread) so generating a
    report — a dozen-plus queries — never stalls presence tracking or slash-command
    handling.

    Args:
        guild: Discord guild
        db: Database manager
        days: Period to report (7 for weekly, 30 for monthly)
        report_types: Enabled report types; controls the Member Changes section

    Returns:
        Discord embed with activity summary
    """
    guild_id = int(guild.id)
    guild_config = db.get_guild_config(guild_id)
    lang = guild_language(guild_config)
    # Guild timezone so "when is the server active" numbers are in its own clock,
    # matching the /user-stats heatmap rather than raw UTC.
    guild_tz = guild_config.get('timezone', 'UTC') if guild_config else 'UTC'

    # Restrict the whole report to the guild's track_only_roles filter (read-time;
    # all members are stored regardless). None => no filter (everyone in scope).
    tracked = db.get_tracked_user_ids(guild_id)
    tracked_set = set(tracked) if tracked is not None else None

    # Get guild-wide message activity stats
    activity_stats = db.get_guild_message_activity_stats(guild_id, days, user_ids=tracked)

    # Get top active users
    top_users = db.get_top_active_users_period(guild_id, days, limit=5, user_ids=tracked)

    # Get daily activity for peak day
    daily_activity = db.get_activity_by_day(guild_id, days, user_ids=tracked, tz_str=guild_tz)
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

    # Peak hour of day (guild timezone, period-scoped). Sourced from
    # get_server_activity_windows so the report agrees with the /user-stats
    # heatmap. No track_only_roles filter — the hourly table is guild-wide,
    # matching that heatmap.
    windows = db.get_server_activity_windows(guild_id, days, guild_tz)
    peak_hour = windows['peak_hour']
    if peak_hour is not None and windows['by_hour'][peak_hour] > 0:
        hour_label = f"{peak_hour:02d}:00-{(peak_hour + 1) % 24:02d}:00"
        embed.description += t('report.peak_hour', lang, hour=hour_label, tz=guild_tz, count=windows['by_hour'][peak_hour])

    embed.description += "\n"

    # Best time to post — busiest contiguous band, mirroring the /user-stats
    # heatmap recommendation. Same minimum-sample gate so a whole-server
    # suggestion is never drawn off a handful of messages.
    if windows['total'] >= 50 and windows['active_days'] >= 7 and windows['recommend']:
        rd, rs, re_ = windows['recommend']
        embed.description += t('report.best_time_header', lang)
        embed.description += t('report.best_time_line', lang, day=weekday_name(rd, lang),
                               start=f"{rs:02d}:00", end=f"{re_ % 24:02d}:00", tz=guild_tz)
        if windows['quiet']:
            qd, qs, qe = windows['quiet']
            embed.description += t('report.best_time_quiet', lang, day=weekday_name(qd, lang),
                                   start=f"{qs:02d}:00", end=f"{qe % 24:02d}:00")
        embed.description += "\n"

    # Member changes - only show the counts whose report type is enabled
    show_joined = 'members' in report_types
    show_left = 'departures' in report_types
    if show_joined or show_left:
        embed.description += t('report.member_changes_header', lang)
        joined = left = None
        if show_joined:
            new_members = db.get_new_members_period(guild_id, days)
            if tracked_set is not None:
                new_members = [m for m in new_members if m['user_id'] in tracked_set]
            joined = len(new_members)
            embed.description += t('report.joined', lang, count=joined)
        if show_left:
            departed_members = db.get_departed_members_period(guild_id, days)
            if tracked_set is not None:
                departed_members = [m for m in departed_members if m['user_id'] in tracked_set]
            left = len(departed_members)
            embed.description += t('report.left', lang, count=left)
        if show_joined and show_left:
            embed.description += t('report.net', lang, count=joined - left)
        # Growth rate over the period (guild-wide, like /user-stats). Complements
        # the raw counts above with the net change as a percentage.
        growth = db.get_member_growth_stats(guild_id, days)
        if growth:
            rate = growth.get('growth_rate', 0)
            indicator = "📈" if rate > 0 else "📉" if rate < 0 else "➡️"
            embed.description += t('report.growth_rate', lang, indicator=indicator, rate=abs(rate))
        embed.description += "\n"

    # Trends: the health-panel deltas, matched to the report cadence (weekly
    # reports compare weeks, monthly reports compare months). Only rendered
    # once the prior window is fully covered by data, so a young guild's
    # report never shows misleading deltas.
    health = db.get_server_health(guild_id)
    if health:
        if days <= 7 and health['weekly_comparable']:
            embed.description += t('report.trends_header_weekly', lang)
            embed.description += t('report.trends_posters', lang, cur=health['posters_7d'],
                                   delta=format_health_delta(health['posters_7d'], health['posters_prev_7d'], lang))
            embed.description += t('report.trends_messages', lang, cur=health['messages_7d'],
                                   delta=format_health_delta(health['messages_7d'], health['messages_prev_7d'], lang))
            embed.description += "\n"
        elif days >= 30 and health['monthly_comparable']:
            embed.description += t('report.trends_header_monthly', lang)
            embed.description += t('report.trends_posters', lang, cur=health['posters_30d'],
                                   delta=format_health_delta(health['posters_30d'], health['posters_prev_30d'], lang))
            embed.description += t('report.trends_messages', lang, cur=health['messages_30d'],
                                   delta=format_health_delta(health['messages_30d'], health['messages_prev_30d'], lang))
            embed.description += t('report.trends_joins', lang, cur=health['joins_30d'],
                                   delta=format_health_delta(health['joins_30d'], health['joins_prev_30d'], lang))
            embed.description += t('report.trends_leaves', lang, cur=health['leaves_30d'],
                                   delta=format_health_delta(health['leaves_30d'], health['leaves_prev_30d'], lang))
            embed.description += t('report.trends_returns', lang, cur=health['returns_30d'],
                                   delta=format_health_delta(health['returns_30d'], health['returns_prev_30d'], lang))
            embed.description += "\n"

    # Participation gap: breadth (who actually posted) plus present-but-silent
    # (lurkers) and never-active (ghosts). Breadth is cadence-matched — 7-day
    # posters for weekly reports, 30-day for monthly — and reuses the health
    # snapshot already fetched above.
    segments = db.get_participation_segments(guild_id, window_days=days)
    have_segments = bool(segments and (segments['lurkers'] or segments['ghosts']))
    breadth_members = health['active_members'] if health else 0
    breadth_posters = (health['posters_7d'] if days <= 7 else health['posters_30d']) if health else 0
    if have_segments or breadth_members:
        embed.description += t('report.participation_header', lang)
        if breadth_members:
            breadth_pct = breadth_posters / breadth_members * 100
            embed.description += t('report.breadth', lang, posters=breadth_posters,
                                   members=breadth_members, pct=round(breadth_pct))
        if have_segments:
            embed.description += t('report.lurkers', lang, count=segments['lurkers'], pct=round(segments['lurker_pct']))
            embed.description += t('report.ghosts', lang, count=segments['ghosts'])
        embed.description += "\n"

    # New-member retention cohorts — opt-in, monthly only. The cohort windows are
    # fixed at 30/60/90 days, so they don't map onto a 7-day weekly period.
    if 'retention' in report_types and days >= 30:
        cohorts = db.get_retention_cohorts(guild_id)
        if cohorts and any(c['total_joined'] > 0 for c in cohorts.values()):
            embed.description += t('report.retention_header', lang)
            period_labels = {
                '30d': t('report.retention_period_30d', lang),
                '60d': t('report.retention_period_60d', lang),
                '90d': t('report.retention_period_90d', lang),
            }
            for period in ('30d', '60d', '90d'):
                data = cohorts.get(period)
                if data and data['total_joined'] > 0:
                    embed.description += t(
                        'report.retention_line', lang,
                        period=period_labels[period], rate=data['retention_rate'],
                        active=data['still_active'], joined=data['total_joined']
                    )
            embed.description += "\n"

    # New-member activation funnel — same opt-in/monthly gate as the cohorts
    # above. Windowed "still posting" share at each age; has its own min-cohort
    # guard, so an empty checkpoint list just means nothing to show.
    if 'retention' in report_types and days >= 30:
        funnel = db.get_activation_funnel(guild_id)
        checkpoints = funnel.get('checkpoints') if funnel else None
        if checkpoints:
            embed.description += t('report.funnel_header', lang)
            for cp in checkpoints:
                embed.description += t(
                    'report.funnel_line', lang,
                    label=cp['label'], rate=cp['rate'],
                    active=cp['active'], matured=cp['matured']
                )
            if funnel.get('largest_dropoff'):
                frm, to = funnel['largest_dropoff']
                embed.description += t('report.funnel_dropoff', lang, from_label=frm, to_label=to)
            embed.description += "\n"

    # Departed-member lifespan — the churn flip-side of the retention cohorts
    # above. Same opt-in/monthly gate; summarised to a single line (the full
    # tenure-bucket chart lives in /user-stats).
    if 'retention' in report_types and days >= 30:
        lifespan = db.get_departure_lifespan(guild_id)
        if lifespan['sample'] >= 3:
            embed.description += t('report.lifespan_header', lang)
            embed.description += t('report.lifespan_line', lang, median=lifespan['median_days'],
                                   avg=lifespan['avg_days'], pct=round(lifespan['early_churn_pct']))
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

    # Pointer to the interactive command for anyone who wants to drill in.
    embed.description += t('report.more_info', lang)

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

                embed = await asyncio.to_thread(generate_activity_report, guild, db, days, report_types)
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

"""Scheduled report generation for LastSeen bot."""

import discord
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from database import DatabaseManager
from bot.utils import create_embed, format_timestamp

logger = logging.getLogger(__name__)

# Rate limiting for report sending
_last_report_send = {}  # guild_id -> timestamp
_RATE_LIMIT_WINDOW = 60  # seconds between reports per guild


async def generate_activity_report(guild: discord.Guild, db: DatabaseManager, days: int) -> discord.Embed:
    """
    Generate activity summary report.
    
    Args:
        guild: Discord guild
        db: Database manager
        days: Period to report (7 for weekly, 30 for monthly)
        
    Returns:
        Discord embed with activity summary
    """
    guild_id = int(guild.id)
    period_name = "Weekly" if days == 7 else "Monthly"
    
    # Get message activity stats
    activity_stats = db.get_message_activity_period(guild_id, None, days)
    
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
    embed = create_embed(f"📊 {period_name} Activity Report - {guild.name}", discord.Color.blue())
    embed.timestamp = datetime.now(timezone.utc)
    
    # Overall statistics
    total_messages = activity_stats.get('total', 0)
    # Guard against division by zero
    avg_per_day = round(total_messages / days, 1) if days > 0 else 0
    
    embed.description = f"**📈 Message Activity ({days} days)**\n"
    embed.description += f"• Total Messages: **{total_messages:,}**\n"
    embed.description += f"• Daily Average: **{avg_per_day:,}**\n"
    
    if peak_day[0]:
        # peak_day[0] is a day name (e.g., 'Monday'), not a timestamp
        embed.description += f"• Peak Day: **{peak_day[0]}** with **{peak_day[1]:,}** messages\n"
    
    embed.description += "\n"
    
    # Top contributors
    if top_users:
        embed.description += "**🏆 Top Contributors**\n"
        for i, user in enumerate(top_users, 1):
            username = user['username'] or "Unknown"
            nickname = user['nickname']
            display = f"{nickname} ({username})" if nickname else username
            embed.description += f"{i}. {display}: **{user['total_messages']:,}** messages\n"
    else:
        embed.description += "**🏆 Top Contributors**\nNo activity recorded\n"
    
    return embed


async def generate_members_report(guild: discord.Guild, db: DatabaseManager, days: int) -> Optional[discord.Embed]:
    """
    Generate new members report.
    
    Args:
        guild: Discord guild
        db: Database manager
        days: Period to report (7 for weekly, 30 for monthly)
        
    Returns:
        Discord embed with new members, or None if no new members
    """
    guild_id = int(guild.id)
    period_name = "Weekly" if days == 7 else "Monthly"
    
    # Get new members
    new_members = db.get_new_members_period(guild_id, days)
    
    if not new_members:
        return None
    
    # Create embed
    embed = create_embed(f"👋 {period_name} New Members - {guild.name}", discord.Color.green())
    embed.timestamp = datetime.now(timezone.utc)
    
    embed.description = f"**{len(new_members)} new member(s) joined in the last {days} days**\n\n"
    
    # List new members (limit to 25 to avoid embed limits)
    for member in new_members[:25]:
        username = member['username'] or "Unknown"
        nickname = member['nickname']
        display = f"{nickname} ({username})" if nickname else username
        join_date_str = format_timestamp(member['join_date'], 'R', guild_id, db)
        position = member.get('join_position', '?')
        
        embed.description += f"• **{display}** - {join_date_str} (#{position})\n"
    
    if len(new_members) > 25:
        embed.description += f"\n*...and {len(new_members) - 25} more*"
    
    return embed


async def generate_departed_report(guild: discord.Guild, db, days: int) -> Optional[List[discord.Embed]]:
    """
    Generate departed members report with pagination support.
    
    Args:
        guild: Discord guild
        db: Database manager
        days: Period to report (7 for weekly, 30 for monthly)
        
    Returns:
        List of Discord embeds with departed members (paginated if >25), or None if no departures
    """
    guild_id = int(guild.id)
    period_name = "Weekly" if days == 7 else "Monthly"
    
    # Get departed members
    departed = db.get_departed_members_period(guild_id, days)
    
    if not departed:
        return None
    
    # Create embed(s) - paginate if more than 25 members
    embeds = []
    page_size = 25
    total_pages = (len(departed) + page_size - 1) // page_size  # Ceiling division
    
    for page in range(total_pages):
        start_idx = page * page_size
        end_idx = min(start_idx + page_size, len(departed))
        page_members = departed[start_idx:end_idx]
        
        # Create embed for this page
        page_title = f"👋 {period_name} Departures - {guild.name}"
        if total_pages > 1:
            page_title += f" (Page {page + 1}/{total_pages})"
        
        embed = create_embed(page_title, discord.Color.orange())
        embed.timestamp = datetime.now(timezone.utc)
        
        if page == 0:
            embed.description = f"**{len(departed)} member(s) left in the last {days} days**\n\n"
        else:
            embed.description = ""
        
        # List departed members for this page
        for member in page_members:
            username = member['username'] or "Unknown"
            nickname = member['nickname']
            display = f"{nickname} ({username})" if nickname else username
            
            if member['left_date']:
                left_date_str = format_timestamp(member['left_date'], 'R', guild_id, db)
        else:
            left_date_str = "Unknown"
        
        if member['last_seen'] and member['last_seen'] > 0:
            last_seen_str = format_timestamp(member['last_seen'], 'R', guild_id, db)
            embed.description += f"• **{display}** - Left {left_date_str} (Last seen: {last_seen_str})\n"
        else:
            embed.description += f"• **{display}** - Left {left_date_str}\n"
    
    if len(departed) > 25:
        embed.description += f"\n*...and {len(departed) - 25} more*"
    
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
    # Rate limiting check
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
            
            embeds = []
            
            # Generate requested reports
            if 'activity' in report_types:
                activity_embed = await generate_activity_report(guild, db, days)
                embeds.append(activity_embed)
            
            if 'members' in report_types:
                members_embed = await generate_members_report(guild, db, days)
                if members_embed:
                    embeds.append(members_embed)
            
            if 'departures' in report_types:
                departures_embeds = await generate_departed_report(guild, db, days)
                if departures_embeds:
                    # departures_embeds is a list, add all of them
                    embeds.extend(departures_embeds)
            
            # Send embeds (Discord allows up to 10 embeds per message)
            if embeds:
                await channel.send(embeds=embeds[:10])
                _last_report_send[guild.id] = datetime.now(timezone.utc).timestamp()
                logger.info(f"Sent scheduled report to {channel.name} in guild {guild.name}")
                return True
            else:
                logger.info(f"No report content to send for guild {guild.name}")
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

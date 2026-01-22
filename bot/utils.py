"""Utility functions for LastSeen bot."""

import discord
import pytz
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def format_timestamp(timestamp: Optional[int], style: str = 'F', guild_id: Optional[int] = None, db = None) -> str:
    """
    Format a Unix timestamp into a Discord timestamp.

    Args:
        timestamp: Unix timestamp (seconds since epoch)
        style: Discord timestamp style
               't' = Short Time (16:20)
               'T' = Long Time (16:20:30)
               'd' = Short Date (20/04/2021)
               'D' = Long Date (20 April 2021)
               'f' = Short Date/Time (20 April 2021 16:20)
               'F' = Long Date/Time (Tuesday, 20 April 2021 16:20)
               'R' = Relative Time (2 months ago)
        guild_id: Optional guild ID to use guild-specific timezone
        db: Optional database manager to fetch guild timezone

    Returns:
        Formatted Discord timestamp string
    """
    if not timestamp or timestamp == 0:
        return "Never"

    try:
        # Create datetime in UTC first (always timezone-aware)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        # Validate that datetime is timezone-aware
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            logger.error(f"Created naive datetime from timestamp {timestamp}")
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to guild timezone if provided
        if guild_id and db:
            try:
                guild_config = db.get_guild_config(guild_id)
                if guild_config:
                    tz_str = guild_config.get('timezone', 'UTC')
                    if tz_str and tz_str != 'UTC':
                        # Validate timezone string before conversion
                        if tz_str in pytz.all_timezones:
                            tz = pytz.timezone(tz_str)
                            dt_converted = dt.astimezone(tz)
                            # Verify conversion was successful
                            if dt_converted.tzinfo is not None:
                                dt = dt_converted
                            else:
                                logger.warning(f"Timezone conversion resulted in naive datetime for {tz_str}")
                        else:
                            logger.warning(f"Invalid timezone in database: {tz_str}")
            except Exception as e:
                logger.error(f"Error during timezone conversion: {e}")
                # Fall back to UTC on any error
        
        # Final validation before formatting
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            logger.error(f"Datetime is naive before formatting, forcing UTC")
            dt = dt.replace(tzinfo=timezone.utc)
        
        return discord.utils.format_dt(dt, style=style)
    except (ValueError, OSError, OverflowError) as e:
        logger.error(f"Error formatting timestamp {timestamp}: {e}")
        return "Invalid date"


def format_relative_time(timestamp: Optional[int], guild_id: Optional[int] = None, db = None) -> str:
    """
    Format a timestamp as relative time (e.g., '2 hours ago').

    Args:
        timestamp: Unix timestamp
        guild_id: Optional guild ID to use guild-specific timezone
        db: Optional database manager to fetch guild timezone

    Returns:
        Relative time string
    """
    return format_timestamp(timestamp, style='R', guild_id=guild_id, db=db)


def get_member_roles(member: discord.Member, exclude_everyone: bool = True) -> list[str]:
    """
    Get a list of role names for a member.

    Args:
        member: Discord member
        exclude_everyone: Whether to exclude @everyone role

    Returns:
        List of role names
    """
    roles = [role.name for role in member.roles]
    if exclude_everyone and '@everyone' in roles:
        roles.remove('@everyone')
    return roles


def parse_user_mention(mention: str) -> Optional[str]:
    """
    Parse a user mention to extract user ID.

    Args:
        mention: String that might be a mention (e.g., '<@123456>')

    Returns:
        User ID as string, or original input if not a mention
    """
    mention = mention.strip()
    if mention.startswith('<@') and mention.endswith('>'):
        # Remove <@ and > and optional ! for nickname mentions
        user_id = mention[2:-1]
        if user_id.startswith('!'):
            user_id = user_id[1:]
        return user_id
    return mention


def create_embed(title: str, color: discord.Color = discord.Color.blue()) -> discord.Embed:
    """
    Create a standard embed with consistent styling.

    Args:
        title: Embed title
        color: Embed color

    Returns:
        Discord embed
    """
    embed = discord.Embed(title=title, color=color)
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def create_error_embed(message: str) -> discord.Embed:
    """
    Create an error embed.

    Args:
        message: Error message

    Returns:
        Discord embed with error styling
    """
    embed = discord.Embed(title="Error", description=message, color=discord.Color.red())
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def create_success_embed(message: str) -> discord.Embed:
    """
    Create a success embed.

    Args:
        message: Success message

    Returns:
        Discord embed with success styling
    """
    embed = discord.Embed(title="Success", description=message, color=discord.Color.green())
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def has_bot_admin_role(member: discord.Member, role_name: str) -> bool:
    """
    Check if a member has the bot admin role.

    Args:
        member: Discord member
        role_name: Name of the bot admin role

    Returns:
        True if member has the role or is guild administrator
    """
    # Guild administrators always have access
    if member.guild_permissions.administrator:
        return True

    # Check for bot admin role
    return discord.utils.get(member.roles, name=role_name) is not None


def can_use_bot_commands(member: discord.Member, guild_config: dict) -> bool:
    """
    Check if a member can use bot user commands based on guild configuration.

    Args:
        member: Discord member
        guild_config: Guild configuration from database

    Returns:
        True if member can use bot commands
    """
    # Guild administrators always have access
    if member.guild_permissions.administrator:
        return True

    # Check if member has bot admin role (from guild config)
    bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin')
    if discord.utils.get(member.roles, name=bot_admin_role_name):
        return True

    # Check if user role is required
    user_role_required = guild_config.get('user_role_required', 0)
    if not user_role_required:
        # User role not required, everyone can use commands
        return True

    # User role is required, check if member has it
    user_role_name = guild_config.get('user_role_name', 'LastSeen User')
    return discord.utils.get(member.roles, name=user_role_name) is not None


def is_channel_allowed(channel_id: int, guild_config: dict) -> bool:
    """
    Check if a channel is allowed for bot commands based on guild configuration.

    Args:
        channel_id: Discord channel ID
        guild_config: Guild configuration from database

    Returns:
        True if channel is allowed for commands
    """
    import json

    # Check if allowed_channels is configured
    allowed_channels_json = guild_config.get('allowed_channels')
    if not allowed_channels_json:
        return True  # If no channel filter, allow all channels

    try:
        allowed_channels = json.loads(allowed_channels_json)
        if not allowed_channels:
            return True  # Empty list means all channels allowed

        # Check if current channel is in allowed list
        return channel_id in allowed_channels
    except:
        return True  # If error parsing, default to allowing


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """
    Split a list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate a string to a maximum length.

    Args:
        text: String to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated

    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix

"""Utility functions for LastSeen bot."""

import discord
from datetime import datetime, timezone
from typing import Optional


def format_timestamp(timestamp: Optional[int], style: str = 'F') -> str:
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

    Returns:
        Formatted Discord timestamp string
    """
    if not timestamp or timestamp == 0:
        return "Never"

    try:
        dt = datetime.fromtimestamp(timestamp)
        return discord.utils.format_dt(dt, style=style)
    except (ValueError, OSError):
        return "Invalid date"


def format_relative_time(timestamp: Optional[int]) -> str:
    """
    Format a timestamp as relative time (e.g., '2 hours ago').

    Args:
        timestamp: Unix timestamp

    Returns:
        Relative time string
    """
    return format_timestamp(timestamp, style='R')


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

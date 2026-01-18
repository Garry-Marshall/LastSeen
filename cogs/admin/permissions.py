"""Shared permission checking utilities for admin commands."""

import discord
import logging
from typing import Optional

from database import DatabaseManager
from bot.utils import has_bot_admin_role, create_error_embed

logger = logging.getLogger(__name__)


async def check_admin_permission(
    interaction: discord.Interaction,
    db: DatabaseManager,
    guild_id: Optional[int] = None
) -> bool:
    """
    Check if user has bot admin permission.

    If user doesn't have permission, sends an error message automatically.

    Args:
        interaction: Discord interaction
        db: Database manager
        guild_id: Guild ID (defaults to interaction.guild_id)

    Returns:
        bool: True if user has permission, False otherwise
    """
    if guild_id is None:
        guild_id = interaction.guild_id

    # Get guild config for bot admin role name
    guild_config = db.get_guild_config(guild_id)
    bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

    # Check permissions
    if not has_bot_admin_role(interaction.user, bot_admin_role_name):
        await interaction.response.send_message(
            embed=create_error_embed(
                f"You need the '{bot_admin_role_name}' role or Administrator permission to use this command."
            ),
            ephemeral=True
        )
        return False

    return True


def get_bot_admin_role_name(db: DatabaseManager, guild_id: int) -> str:
    """
    Get the configured bot admin role name for a guild.

    Args:
        db: Database manager
        guild_id: Guild ID

    Returns:
        str: Bot admin role name (defaults to 'LastSeen Admin')
    """
    guild_config = db.get_guild_config(guild_id)
    return guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

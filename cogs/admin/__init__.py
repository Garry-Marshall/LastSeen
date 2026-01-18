"""Admin cog package for LastSeen bot."""

import logging
from discord.ext import commands

from .admin_cog import AdminCog
from .config_view import ConfigView
from .channel_config import ChannelModal, InactiveDaysModal
from .role_config import BotAdminRoleModal, UserRoleModal, TrackOnlyRolesModal
from .channel_filter import AllowedChannelsModal
from .permissions import check_admin_permission, get_bot_admin_role_name

logger = logging.getLogger(__name__)


async def setup(bot: commands.Bot):
    """
    Setup function for loading the cog.

    Args:
        bot: Discord bot instance
    """
    db = bot.db
    config = bot.config
    await bot.add_cog(AdminCog(bot, db, config))
    logger.info("AdminCog loaded")


__all__ = [
    'AdminCog',
    'ConfigView',
    'ChannelModal',
    'InactiveDaysModal',
    'BotAdminRoleModal',
    'UserRoleModal',
    'TrackOnlyRolesModal',
    'AllowedChannelsModal',
    'check_admin_permission',
    'get_bot_admin_role_name',
    'setup'
]

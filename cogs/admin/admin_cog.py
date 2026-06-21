"""Admin commands cog for bot configuration."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime, timezone
import psutil
import sys
import os

from database import DatabaseManager
from bot.utils import create_embed, create_error_embed, has_bot_admin_role
from bot.locale import t, guild_language
from .config_view import ConfigView
from .permissions import check_admin_permission, get_bot_admin_role_name

logger = logging.getLogger(__name__)


class AdminCog(commands.Cog):
    """Cog for admin commands."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        """
        Initialize admin cog.

        Args:
            bot: Discord bot instance
            db: Database manager
            config: Bot configuration
        """
        self.bot = bot
        self.db = db
        self.config = config

    @app_commands.command(name="config", description="⚙️ Configure bot settings (Admin only)")
    @app_commands.guild_only()
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: i.user.id)
    async def config(self, interaction: discord.Interaction):
        """
        Display bot configuration interface.

        Args:
            interaction: Discord interaction
        """
        if not await check_admin_permission(interaction, self.db):
            return

        lang = guild_language(self.db.get_guild_config(interaction.guild_id))

        # Create embed
        embed = create_embed(t("admin.config.title", lang), discord.Color.gold())
        embed.description = t("admin.config.description", lang)

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    @app_commands.command(name="help", description="❓ Show bot information and available commands")
    @app_commands.guild_only()
    async def help(self, interaction: discord.Interaction):
        """
        Display bot information and list of available commands.
        Shows all commands for admins, only user commands for 'LastSeen Users' role.

        Args:
            interaction: Discord interaction
        """
        # Check if user is admin (without auto-responding)
        guild_config = self.db.get_guild_config(interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        is_admin = has_bot_admin_role(interaction.user, bot_admin_role_name)
        lang = guild_language(guild_config)

        # Check if user has 'LastSeen Users' role
        user_role_name = guild_config.get('user_role_name', 'LastSeen User') if guild_config else 'LastSeen User'
        has_user_role = discord.utils.get(interaction.user.roles, name=user_role_name) is not None

        # If neither admin nor has user role, deny access
        if not is_admin and not has_user_role:
            await interaction.response.send_message(
                embed=create_error_embed(t("errors.no_user_or_admin_permission", lang, role=user_role_name), lang),
                ephemeral=True
            )
            return

        # Create help embed
        embed = create_embed(t("admin.help.title", lang), discord.Color.blue())
        embed.description = t("admin.help.description", lang)

        # User Commands (shown to both admin and users)
        embed.add_field(
            name=t("admin.help.user_commands_title", lang),
            value=t("admin.help.user_commands", lang),
            inline=False
        )

        # Admin Commands (only shown to admins)
        if is_admin:
            embed.add_field(
                name=t("admin.help.admin_commands_title", lang),
                value=t("admin.help.admin_commands", lang),
                inline=False
            )

            embed.add_field(
                name=t("admin.help.search_title", lang),
                value=t("admin.help.search", lang),
                inline=False
            )

            embed.set_footer(text=t("admin.help.footer_admin", lang, role=bot_admin_role_name))
        else:
            embed.set_footer(text=t("admin.help.footer_user", lang, role=user_role_name))

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")


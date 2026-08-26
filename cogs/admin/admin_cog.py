"""Admin commands cog for bot configuration."""

import asyncio
import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime, timezone
from typing import Optional
import psutil
import sys
import os

from database import DatabaseManager
from bot.utils import create_embed, create_error_embed, has_bot_admin_role, can_use_bot_commands
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
    @app_commands.checks.cooldown(1, 30.0, key=lambda i: (i.guild_id, i.user.id))
    async def config(self, interaction: discord.Interaction):
        """
        Display bot configuration interface.

        Args:
            interaction: Discord interaction
        """
        if not await check_admin_permission(interaction, self.db):
            return

        lang = guild_language(await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id))

        # Create embed
        embed = create_embed(t("admin.config.title", lang), discord.Color.gold())
        embed.description = t("admin.config.description", lang)

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    # Commands that have a dedicated `/help <command>` detail page. Extend this
    # list (and add the matching locale keys + a branch below) as detailed help
    # is written for more commands.
    DETAILED_HELP = ['watch', 'search']

    async def help_command_autocomplete(self, interaction: discord.Interaction, current: str):
        """Suggest command names that have a detailed `/help <command>` page."""
        cur = current.lower().lstrip('/')
        return [app_commands.Choice(name=c, value=c) for c in self.DETAILED_HELP if cur in c][:25]

    @app_commands.command(name="help", description="❓ Show bot information and available commands")
    @app_commands.describe(command="Optional: a command name for detailed help (e.g. watch)")
    @app_commands.autocomplete(command=help_command_autocomplete)
    @app_commands.guild_only()
    async def help(self, interaction: discord.Interaction, command: Optional[str] = None):
        """
        Display bot information and list of available commands.
        Shows all commands for admins, only user commands for 'LastSeen Users' role.
        With a command argument, shows a detailed help page for that command.

        Args:
            interaction: Discord interaction
            command: Optional command name for a detailed help page
        """
        # Check if user is admin (without auto-responding)
        guild_config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        user_role_name = guild_config.get('user_role_name', 'LastSeen User') if guild_config else 'LastSeen User'
        is_admin = has_bot_admin_role(interaction.user, bot_admin_role_name)
        lang = guild_language(guild_config)

        # Respect the user_role_required toggle: when the user role isn't required,
        # everyone can use help (consistent with can_use_bot_commands elsewhere).
        if guild_config and not can_use_bot_commands(interaction.user, guild_config):
            await interaction.response.send_message(
                embed=create_error_embed(t("errors.no_user_or_admin_permission", lang, role=user_role_name), lang),
                ephemeral=True
            )
            return

        # Detailed help for a specific command. These are all admin-only
        # commands, so their detail pages require the bot-admin role too.
        if command:
            key = command.strip().lstrip('/').lower()
            detail_builders = {
                'watch': self._build_watch_help,
                'search': self._build_search_help,
            }
            if key in detail_builders:
                if not is_admin:
                    await interaction.response.send_message(
                        embed=create_error_embed(t("errors.no_permission", lang, role=bot_admin_role_name), lang),
                        ephemeral=True
                    )
                    return
                await interaction.response.send_message(embed=detail_builders[key](lang), ephemeral=True)
                logger.info(f"User {interaction.user} viewed /help {key} in guild {interaction.guild.name}")
                return
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.help.detail_unknown", lang, command=key), lang),
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

            embed.add_field(
                name=t("admin.help.watch_section_title", lang),
                value=t("admin.help.watch_section", lang),
                inline=False
            )

            embed.set_footer(text=t("admin.help.footer_admin", lang, role=bot_admin_role_name))
        else:
            embed.set_footer(text=t("admin.help.footer_user", lang, role=user_role_name))

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")

    def _build_watch_help(self, lang: str) -> discord.Embed:
        """Build the detailed `/help watch` embed."""
        embed = create_embed(t("admin.help.watch_detail_title", lang), discord.Color.blurple())
        embed.description = t("admin.help.watch_detail_desc", lang)
        embed.add_field(
            name=t("admin.help.watch_detail_commands_title", lang),
            value=t("admin.help.watch_detail_commands", lang),
            inline=False
        )
        embed.add_field(
            name=t("admin.help.watch_detail_examples_title", lang),
            value=t("admin.help.watch_detail_examples", lang),
            inline=False
        )
        embed.add_field(
            name=t("admin.help.watch_detail_notes_title", lang),
            value=t("admin.help.watch_detail_notes", lang),
            inline=False
        )
        return embed

    def _build_search_help(self, lang: str) -> discord.Embed:
        """Build the detailed `/help search` embed."""
        embed = create_embed(t("admin.help.search_detail_title", lang), discord.Color.blurple())
        embed.description = t("admin.help.search_detail_desc", lang)
        embed.add_field(
            name=t("admin.help.search_detail_filters_title", lang),
            value=t("admin.help.search_detail_filters", lang),
            inline=False
        )
        embed.add_field(
            name=t("admin.help.search_detail_examples_title", lang),
            value=t("admin.help.search_detail_examples", lang),
            inline=False
        )
        embed.add_field(
            name=t("admin.help.search_detail_notes_title", lang),
            value=t("admin.help.search_detail_notes", lang),
            inline=False
        )
        return embed


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
    async def config(self, interaction: discord.Interaction):
        """
        Display bot configuration interface.

        Args:
            interaction: Discord interaction
        """
        if not await check_admin_permission(interaction, self.db):
            return

        # Create embed
        embed = create_embed("Bot Configuration", discord.Color.gold())
        embed.description = (
            "Configure bot settings for this server.\n\n"
            "**🚀 First time here?** Click **Quick Setup** for a guided walkthrough!\n\n"
            "**📢 Set Notification Channel:** Choose where member leave notifications are posted\n"
            "**📅 Set Inactive Days:** Set the threshold for /inactive command\n"
            "**🌍 Set Timezone:** Configure server timezone (e.g., America/New_York)\n"
            "**👑 Set Bot Admin Role:** Set which role can manage bot settings\n"
            "**🔐 Toggle User Role Required:** Enable/disable role requirement for using bot commands\n"
            "**👤 Set User Role:** Set which role can use bot commands (when required)\n"
            "**🎯 Set Track Only Roles:** Only track members with specific roles (optional)\n"
            "**📝 Set Allowed Channels:** Restrict bot commands to specific channels (optional)\n"
            "**📊 Configure Reports:** Set up automated weekly/monthly activity reports\n"
            "**🚫 Disable Reports:** Turn off scheduled reports for this server\n"
            "**🚀 Quick Setup:** Guided wizard for first-time setup\n"
            "**⚙️ View Config:** View current configuration settings"
        )

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
        
        # Check if user has 'LastSeen Users' role
        user_role_name = guild_config.get('user_role_name', 'LastSeen User') if guild_config else 'LastSeen User'
        has_user_role = discord.utils.get(interaction.user.roles, name=user_role_name) is not None
        
        # If neither admin nor has user role, deny access
        if not is_admin and not has_user_role:
            await interaction.response.send_message(
                embed=create_error_embed(f"You need the '{user_role_name}' role or admin permissions to use this command."),
                ephemeral=True
            )
            return

        # Create help embed
        embed = create_embed("LastSeen Bot - Help", discord.Color.blue())
        embed.description = (
            "Track member activity, monitor server statistics, and automate reports. "
            "Use `/config` to configure bot settings.\n\n"
            "**Quick Start:**\n"
            "• Check when someone was last online: `/lastseen @user`\n"
            "• View member details: `/whois @user`\n"
            "• List inactive members: `/inactive`\n"
            "• Configure bot settings: `/config` (Admin only)"
        )

        # User Commands (shown to both admin and users)
        embed.add_field(
            name="👥 User Commands",
            value=(
                "`/whois <user>` - Show user info, roles, join date, last seen\n"
                "`/lastseen <user>` or `/seen <user>` - When user was last online\n"
                "`/inactive` - List members offline beyond threshold\n"
                "`/inactive <days>` - List members offline for <days> days\n"
                "`/chat-history <user>` - Show message posting stats for the last year\n"
                "`/user-stats` - Interactive statistics dashboard with analytics\n"
            ),
            inline=False
        )

        # Admin Commands (only shown to admins)
        if is_admin:
            embed.add_field(
                name="⚙️ Admin Commands",
                value=(
                    "`/config` - Configure bot settings:\n"
                    "  • Notification channel & inactive days\n"
                    "  • Admin/user role permissions\n"
                    "  • Track only specific roles (optional)\n"
                    "  • Allowed channels (restrict commands)\n"
                    "`/role-history <user>` - View role change history for a member\n"
                ),
                inline=False
            )

            embed.add_field(
                name="🔍 Search & Filter",
                value=(
                    "`/search` - Advanced member search with filters\n"
                    "  Examples:\n"
                    "  • `/search roles:@Moderator status:online`\n"
                    "  • `/search inactive:>30 activity:<10`\n"
                    "  • `/search joined:>2025-01-01 export:csv`\n"
                    "  Filters: roles, status, inactive, activity, joined, username\n"
                    "  Export: csv or txt format"
                ),
                inline=False
            )
            
            embed.set_footer(text=f"Bot Admin Role: {bot_admin_role_name} • Showing all commands")
        else:
            embed.set_footer(text=f"User role: {user_role_name} • Showing user commands only")

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")


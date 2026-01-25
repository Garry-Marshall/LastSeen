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
            "**👑 Set Bot Admin Role:** Set which role can manage bot settings\n"
            "**🔐 Toggle User Role Required:** Enable/disable role requirement for using bot commands\n"
            "**👤 Set User Role:** Set which role can use bot commands (when required)\n"
            "**🎯 Set Track Only Roles:** Only track members with specific roles (optional)\n"
            "**📝 Set Allowed Channels:** Restrict bot commands to specific channels (optional)\n"
            "**🗑️ Set Retention Days:** Configure message activity auto-cleanup period (default: 365 days)\n"
            "**🌍 Set Timezone:** Configure server timezone (e.g., America/New_York)\n"
            "**📊 Configure Reports:** Set up automated weekly/monthly activity reports\n"
            "**🚫 Disable Reports:** Turn off scheduled reports for this server\n"
            "**🔄 Update All Members:** Scan and update all current members in the database\n"
            "**🚀 Quick Setup:** Guided wizard for first-time setup\n"
            "**⚙️ View Config:** View current configuration settings"
        )

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    @app_commands.command(name="server-stats", description="📈 View server activity and system status")
    @app_commands.guild_only()
    async def server_stats(self, interaction: discord.Interaction):
        """
        Display comprehensive server statistics including activity metrics and system resources.

        Args:
            interaction: Discord interaction
        """
        # Check if user has permission (admin or user role)
        guild_config = self.db.get_guild_config(interaction.guild_id)
        
        if guild_config and not has_bot_admin_role(interaction.user, guild_config.get('bot_admin_role_name', 'LastSeen Admin')):
            # Not admin, check for user role
            user_role_required = guild_config.get('user_role_required', 0)
            if user_role_required:
                user_role_name = guild_config.get('user_role_name', 'LastSeen User')
                if not discord.utils.get(interaction.user.roles, name=user_role_name):
                    await interaction.response.send_message(
                        f"❌ You need the '{user_role_name}' role or admin permissions to use this command.",
                        ephemeral=True
                    )
                    return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if interaction.guild is None:
            await interaction.followup.send(
                "❌ This command can only be used inside a server.",
                ephemeral=True
            )
            return

        try:
            # Get activity statistics
            activity_stats = self.db.get_activity_stats(interaction.guild_id)
            guild_stats = self.db.get_guild_stats(interaction.guild_id)
            db_health = self.db.get_database_health()

            # Create embed
            embed = create_embed(f"📊 {interaction.guild.name} - Server Status", discord.Color.blue())

            # === SYSTEM RESOURCES ===
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=0.1)
            thread_count = process.num_threads()
            
            # Bot uptime
            if self.bot.start_time:
                uptime_delta = datetime.now(timezone.utc) - self.bot.start_time
                days = uptime_delta.days
                hours, remainder = divmod(uptime_delta.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                if days > 0:
                    uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
                elif hours > 0:
                    uptime_str = f"{hours}h {minutes}m {seconds}s"
                else:
                    uptime_str = f"{minutes}m {seconds}s"
            else:
                uptime_str = "Unknown"
            
            # Latency
            latency_ms = round(self.bot.latency * 1000)
            
            # Database status
            db_status = "✅ Healthy" if db_health['status'] == 'healthy' else "❌ Unhealthy"
            
            # Python version
            python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
            
            system_info = (
                f"```\n"
                f"Memory:    {memory_mb:.1f} MB\n"
                f"CPU:       {cpu_percent:.1f}%\n"
                f"Threads:   {thread_count}\n"
                f"Uptime:    {uptime_str}\n"
                f"Latency:   {latency_ms} ms\n"
                f"Python:    {python_version}\n"
                f"Database:  {db_status}\n"
                f"DB Size:   {db_health['file_size_mb']:.2f} MB\n"
                f"```"
            )
            
            embed.add_field(
                name="⚙️ System Resources",
                value=system_info,
                inline=False
            )

            # === PRESENCE STATUS ===
            # Get actual online count from Discord (not database)
            # Database may have stale last_seen=0 for users who were online before bot restart
            actual_online = sum(1 for m in interaction.guild.members 
                              if not m.bot and m.status != discord.Status.offline)
            
            total_tracked = guild_stats['active_members']
            online_count = actual_online  # Use actual Discord status
            offline_count = total_tracked - actual_online
            
            if total_tracked > 0:
                online_pct = (online_count / total_tracked) * 100
            else:
                online_pct = 0

            embed.add_field(
                name="📡 Current Presence",
                value=(
                    f"**Total Tracked:** {total_tracked:,} members\n"
                    f"**🟢 Online:** {online_count:,} ({online_pct:.1f}%)\n"
                    f"**⚫ Offline:** {offline_count:,}"
                ),
                inline=True
            )

            # === MEMBER TRACKING ===
            left_members = guild_stats['inactive_members']
            embed.add_field(
                name="👥 Member Status",
                value=(
                    f"**Active:** {guild_stats['active_members']:,}\n"
                    f"**Left Server:** {left_members:,}\n"
                    f"**Total Tracked:** {guild_stats['total_members']:,}"
                ),
                inline=True
            )

            # === ACTIVITY BREAKDOWN (Chart) ===
            def create_bar(count: int, max_count: int, length: int = 20) -> str:
                if max_count == 0:
                    return "░" * length
                filled = int((count / max_count) * length)
                return "█" * filled + "░" * (length - filled)

            max_offline = max(
                activity_stats['offline_1h'],
                activity_stats['offline_24h'],
                activity_stats['offline_7d'],
                activity_stats['offline_30d'],
                activity_stats['offline_30d_plus'],
                1
            )

            chart = (
                f"```\n"
                f"<1 hour  {create_bar(activity_stats['offline_1h'], max_offline)} {activity_stats['offline_1h']:>5,}\n"
                f"<24 hrs  {create_bar(activity_stats['offline_24h'], max_offline)} {activity_stats['offline_24h']:>5,}\n"
                f"<7 days  {create_bar(activity_stats['offline_7d'], max_offline)} {activity_stats['offline_7d']:>5,}\n"
                f"<30 days {create_bar(activity_stats['offline_30d'], max_offline)} {activity_stats['offline_30d']:>5,}\n"
                f"30+ days {create_bar(activity_stats['offline_30d_plus'], max_offline)} {activity_stats['offline_30d_plus']:>5,}\n"
                f"```"
            )

            embed.add_field(
                name="⏰ Last Seen Distribution",
                value=chart,
                inline=False
            )

            embed.set_footer(text=f"📅 Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"User {interaction.user} viewed server stats in guild {interaction.guild.name}")

        except Exception as e:
            logger.error(f"Failed to generate server stats: {e}", exc_info=True)
            await interaction.followup.send(
                embed=create_error_embed(f"Failed to generate server statistics: {str(e)}"),
                ephemeral=True
            )

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
                "`/server-stats` - System status and activity metrics\n"
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


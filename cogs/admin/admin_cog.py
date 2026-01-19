"""Admin commands cog for bot configuration."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime, timezone

from database import DatabaseManager
from bot.utils import create_embed, create_error_embed
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

    @app_commands.command(name="config", description="Configure bot settings (Admin only)")
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
            "Use the buttons below to configure the bot settings for this server.\n\n"
            "**📢 Set Notification Channel:** Choose where member leave notifications are posted\n"
            "**📅 Set Inactive Days:** Set the threshold for /inactive command\n"
            "**👑 Set Bot Admin Role:** Set which role can manage bot settings\n"
            "**🔐 Toggle User Role Required:** Enable/disable role requirement for using bot commands\n"
            "**👤 Set User Role:** Set which role can use bot commands (when required)\n"
            "**🎯 Set Track Only Roles:** Only track members with specific roles (optional)\n"
            "**📝 Set Allowed Channels:** Restrict bot commands to specific channels (optional)\n"
            "**🔄 Update All Members:** Scan and update all current members in the database\n"
            "**⚙️ View Config:** View current configuration settings"
        )

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    @app_commands.command(name="health", description="Check bot health status (Admin only)")
    @app_commands.guild_only()
    async def health(self, interaction: discord.Interaction):
        """
        Display bot health and status information.

        Args:
            interaction: Discord interaction
        """

        if not await check_admin_permission(interaction, self.db):
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Create health status embed
        embed = create_embed("Bot Health Status", discord.Color.green())

        # Bot uptime
        if self.bot.start_time:
            uptime_delta = datetime.now(timezone.utc) - self.bot.start_time
            days = uptime_delta.days
            hours, remainder = divmod(uptime_delta.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            if days > 0:
                uptime_str = f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                uptime_str = f"{hours}h {minutes}m"
            else:
                uptime_str = f"{minutes}m"
        else:
            uptime_str = "Unknown"

        # Bot latency
        latency_ms = round(self.bot.latency * 1000)

        embed.add_field(
            name="🤖 Bot Status",
            value=f"**Status:** ✅ Online\n**Uptime:** {uptime_str}\n**Latency:** {latency_ms}ms",
            inline=False
        )

        # Database health
        db_health = self.db.get_database_health()
        if db_health['status'] == 'healthy':
            status_icon = "✅"
            status_text = "Healthy"
        else:
            status_icon = "❌"
            status_text = "Unhealthy"

        embed.add_field(
            name="💾 Database",
            value=f"**Status:** {status_icon} {status_text}\n**Size:** {db_health['file_size_mb']} MB",
            inline=False
        )

        # Guild-specific stats
        guild_stats = self.db.get_guild_stats(interaction.guild_id)
        guild_config = self.db.get_guild_config(interaction.guild_id)

        # Check configuration completeness
        config_complete = False
        if guild_config and guild_config['notification_channel_id']:
            config_complete = True

        config_status = "✅ Complete" if config_complete else "⚠️ Incomplete"

        embed.add_field(
            name="🏰 This Guild",
            value=(
                f"**Tracked Members:** {guild_stats['total_members']}\n"
                f"**Active:** {guild_stats['active_members']} | "
                f"**Left:** {guild_stats['inactive_members']}\n"
                f"**Configuration:** {config_status}"
            ),
            inline=False
        )

        await interaction.followup.send(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} checked health status in guild {interaction.guild.name}")

    @app_commands.command(name="server-stats", description="View server activity statistics")
    @app_commands.guild_only()
    async def server_stats(self, interaction: discord.Interaction):
        """
        Display server activity statistics and metrics.

        Args:
            interaction: Discord interaction
        """
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

            # Create embed
            embed = create_embed(f"📊 {interaction.guild.name} - Activity Statistics", discord.Color.blue())

            # Current Status Section
            total_tracked = guild_stats['active_members']
            online_count = activity_stats['currently_online']
            offline_count = activity_stats['currently_offline']

            embed.add_field(
                name="📡 Current Status",
                value=(
                    f"**Total Tracked:** {total_tracked} members\n"
                    f"**🟢 Online:** {online_count}\n"
                    f"**⚫ Offline:** {offline_count}"
                ),
                inline=False
            )

            # Activity Breakdown Section
            embed.add_field(
                name="⏰ Offline Duration Breakdown",
                value=(
                    f"**Last hour:** {activity_stats['offline_1h']} members\n"
                    f"**Last 24 hours:** {activity_stats['offline_24h']} members\n"
                    f"**Last 7 days:** {activity_stats['offline_7d']} members\n"
                    f"**Last 30 days:** {activity_stats['offline_30d']} members\n"
                    f"**30+ days ago:** {activity_stats['offline_30d_plus']} members"
                ),
                inline=False
            )

            # Activity Percentages (if we have members)
            if total_tracked > 0:
                online_pct = round((online_count / total_tracked) * 100, 1)
                recent_pct = round((activity_stats['offline_24h'] / total_tracked) * 100, 1)

                embed.add_field(
                    name="📈 Activity Metrics",
                    value=(
                        f"**Online Rate:** {online_pct}% currently active\n"
                        f"**Recent Activity:** {recent_pct}% seen in last 24h"
                    ),
                    inline=False
                )

            # Member Tracking Info
            left_members = guild_stats['inactive_members']
            embed.add_field(
                name="👥 Member Tracking",
                value=(
                    f"**Active Members:** {guild_stats['active_members']}\n"
                    f"**Left Server:** {left_members}\n"
                    f"**Total Tracked:** {guild_stats['total_members']}"
                ),
                inline=False
            )

            # Create simple text-based bar chart for visualization
            def create_bar(count: int, max_count: int, length: int = 20) -> str:
                if max_count == 0:
                    return "▱" * length
                filled = int((count / max_count) * length)
                return "▰" * filled + "▱" * (length - filled)

            max_offline = max(
                activity_stats['offline_1h'],
                activity_stats['offline_24h'],
                activity_stats['offline_7d'],
                activity_stats['offline_30d'],
                activity_stats['offline_30d_plus'],
                1  # Avoid division by zero
            )

            chart = (
                f"```\n"
                f"Activity Distribution\n"
                f"{'─' * 30}\n"
                f"<1h   {create_bar(activity_stats['offline_1h'], max_offline)} {activity_stats['offline_1h']}\n"
                f"<24h  {create_bar(activity_stats['offline_24h'], max_offline)} {activity_stats['offline_24h']}\n"
                f"<7d   {create_bar(activity_stats['offline_7d'], max_offline)} {activity_stats['offline_7d']}\n"
                f"<30d  {create_bar(activity_stats['offline_30d'], max_offline)} {activity_stats['offline_30d']}\n"
                f"30d+  {create_bar(activity_stats['offline_30d_plus'], max_offline)} {activity_stats['offline_30d_plus']}\n"
                f"```"
            )

            embed.add_field(
                name="📊 Visual Breakdown",
                value=chart,
                inline=False
            )

            embed.set_footer(text=f"Stats generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"User {interaction.user} viewed server stats in guild {interaction.guild.name}")

        except Exception as e:
            logger.error(f"Failed to generate server stats: {e}")
            await interaction.followup.send(
                embed=create_error_embed(f"Failed to generate server statistics: {str(e)}"),
                ephemeral=True
            )

    @app_commands.command(name="help", description="Show bot information and available commands (Admin only)")
    @app_commands.guild_only()
    async def help(self, interaction: discord.Interaction):
        """
        Display bot information and list of all available commands.

        Args:
            interaction: Discord interaction
        """
        if not await check_admin_permission(interaction, self.db):
            return

        # Get bot admin role name for footer
        bot_admin_role_name = get_bot_admin_role_name(self.db, interaction.guild_id)

        # Create help embed
        embed = create_embed("LastSeen Bot - Help", discord.Color.blue())
        embed.description = (
            "Track user activity, monitor presence, and manage member data across your server."
        )

        # User Commands
        embed.add_field(
            name="👥 User Commands",
            value=(
                "`/whois <user>` - Show user info, roles, join date, last seen\n"
                "`/lastseen <user>` or `/seen <user>` - When user was last online\n"
                "`/inactive` - List members offline beyond threshold\n"
                "`/server-stats` - Server activity metrics and charts"
            ),
            inline=False
        )

        # Admin Commands
        embed.add_field(
            name="⚙️ Admin Commands",
            value=(
                "`/config` - Configure bot settings:\n"
                "  • Notification channel & inactive days\n"
                "  • Admin/user role permissions\n"
                "  • Track only specific roles (optional)\n"
                "  • Restrict commands to channels (optional)\n"
                "  • Update member database\n"
                "`/health` - Bot status & database health\n"
                "`/help` - Show this help message"
            ),
            inline=False
        )

        # Permissions & Features
        embed.add_field(
            name="🔐 Permissions & Features",
            value=(
                "**Access Control**\n"
                "• Admin: Bot Admin role or Administrator\n"
                "• Users: Configurable role & channel restrictions\n\n"
                "**Key Features**\n"
                "• Presence tracking • Activity statistics\n"
                "• Role-based visibility • Channel restrictions\n"
                "• Multi-guild support • Privacy focused"
            ),
            inline=False
        )

        # Footer with helpful info
        embed.set_footer(text=f"Bot Admin Role: {bot_admin_role_name}")

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")

"""Admin commands cog for bot configuration."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime, timezone

from database import DatabaseManager
from bot.utils import (
    has_bot_admin_role,
    create_embed,
    create_error_embed,
    create_success_embed,
    get_member_roles
)

logger = logging.getLogger(__name__)


class ConfigView(discord.ui.View):
    """Interactive view for bot configuration."""

    def __init__(self, db: DatabaseManager, guild_id: int, config):
        """
        Initialize config view.

        Args:
            db: Database manager
            guild_id: Discord guild ID
            config: Bot configuration
        """
        super().__init__(timeout=300)  # 5 minute timeout
        self.db = db
        self.guild_id = guild_id
        self.config = config

    @discord.ui.button(label="Set Notification Channel", style=discord.ButtonStyle.primary, emoji="📢")
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the notification channel."""
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for channel input
        modal = ChannelModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Inactive Days", style=discord.ButtonStyle.primary, emoji="📅")
    async def set_inactive_days(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the inactive days threshold."""
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for days input
        modal = InactiveDaysModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Update All Members", style=discord.ButtonStyle.success, emoji="🔄")
    async def update_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to enumerate and update all members."""
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        added_count = 0
        updated_count = 0

        try:
            # First, ensure guild name is correct
            self.db.update_guild_name(guild.id, guild.name)

            for member in guild.members:
                if member.bot:
                    continue

                roles = get_member_roles(member)
                join_date = int(member.joined_at.timestamp()) if member.joined_at else int(datetime.now(timezone.utc).timestamp())
                nickname = member.display_name if member.display_name != str(member) else None

                if self.db.member_exists(guild.id, member.id):
                    # Update existing member
                    self.db.update_member_username(guild.id, member.id, str(member))
                    self.db.update_member_nickname(guild.id, member.id, nickname)
                    self.db.update_member_roles(guild.id, member.id, roles)
                    updated_count += 1
                else:
                    # Add new member
                    self.db.add_member(
                        guild_id=guild.id,
                        user_id=member.id,
                        username=str(member),
                        nickname=nickname,
                        join_date=join_date,
                        roles=roles
                    )
                    added_count += 1

            embed = create_success_embed(
                f"Successfully updated member database!\n\n"
                f"**Added:** {added_count} new members\n"
                f"**Updated:** {updated_count} existing members"
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"Updated members in guild {guild.name}: {added_count} added, {updated_count} updated")

        except Exception as e:
            logger.error(f"Failed to update members: {e}")
            await interaction.followup.send(
                embed=create_error_embed(f"Failed to update members: {str(e)}"),
                ephemeral=True
            )

    @discord.ui.button(label="View Config", style=discord.ButtonStyle.secondary, emoji="⚙️")
    async def view_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to view current configuration."""
        guild_config = self.db.get_guild_config(self.guild_id)

        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed("Guild configuration not found."),
                ephemeral=True
            )
            return

        embed = create_embed("Current Configuration", discord.Color.blue())

        # Notification channel
        channel_id = guild_config['notification_channel_id']
        if channel_id:
            channel = interaction.guild.get_channel(channel_id)
            channel_str = channel.mention if channel else f"Unknown ({channel_id})"
        else:
            channel_str = "Not set"
        embed.add_field(name="Notification Channel", value=channel_str, inline=False)

        # Inactive days
        embed.add_field(
            name="Inactive Days Threshold",
            value=f"{guild_config['inactive_days']} days",
            inline=False
        )

        # Member count
        member_count = len(self.db.get_all_guild_members(self.guild_id))
        embed.add_field(name="Tracked Members", value=str(member_count), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


class ChannelModal(discord.ui.Modal, title="Set Notification Channel"):
    """Modal for setting the notification channel."""

    channel_input = discord.ui.TextInput(
        label="Channel Name, ID, or Mention",
        placeholder="e.g., general, #general, or 1234567890",
        required=True,
        max_length=100
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        channel_str = self.channel_input.value.strip()
        channel = None

        # Try different parsing methods
        # 1. Check if it's a mention like <#1234567890>
        if channel_str.startswith('<#') and channel_str.endswith('>'):
            try:
                channel_id = int(channel_str[2:-1])
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                pass

        # 2. Check if it's a numeric ID
        if not channel:
            try:
                channel_id = int(channel_str)
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                pass

        # 3. Try to find channel by name (remove # if present)
        if not channel:
            search_name = channel_str.lstrip('#').lower()
            channel = discord.utils.get(
                interaction.guild.text_channels,
                name=search_name
            )

        # If still not found, return error
        if not channel:
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"Channel '{channel_str}' not found. Please use channel name (e.g., 'general'), "
                    f"mention (e.g., '#general'), or ID."
                ),
                ephemeral=True
            )
            return

        # Validate channel type - must be a text channel
        if not isinstance(channel, discord.TextChannel):
            channel_type = type(channel).__name__
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"Invalid channel type. '{channel.name}' is a {channel_type}, "
                    f"but notification channels must be text channels."
                ),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_notification_channel(self.guild_id, channel.id, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"Notification channel set to {channel.mention}"),
                ephemeral=True
            )
            logger.info(f"Notification channel set to {channel.name} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update notification channel."),
                ephemeral=True
            )


class InactiveDaysModal(discord.ui.Modal, title="Set Inactive Days Threshold"):
    """Modal for setting the inactive days threshold."""

    days_input = discord.ui.TextInput(
        label="Number of Days",
        placeholder="Enter number of days (e.g., 10)",
        required=True,
        max_length=3
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        days_str = self.days_input.value.strip()

        try:
            days = int(days_str)
            if days < 1 or days > 365:
                raise ValueError("Days must be between 1 and 365")
        except ValueError as e:
            error_msg = str(e) if "between" in str(e) else "Please enter a valid number between 1 and 365."
            await interaction.response.send_message(
                embed=create_error_embed(error_msg),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_inactive_days(self.guild_id, days, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"Inactive days threshold set to {days} days"),
                ephemeral=True
            )
            logger.info(f"Inactive days threshold set to {days} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update inactive days threshold."),
                ephemeral=True
            )


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
    async def config(self, interaction: discord.Interaction):
        """
        Display bot configuration interface.

        Args:
            interaction: Discord interaction
        """
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{self.config.bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
            return

        # Create embed
        embed = create_embed("Bot Configuration", discord.Color.gold())
        embed.description = (
            "Use the buttons below to configure the bot settings for this server.\n\n"
            "**📢 Set Notification Channel:** Choose where member leave notifications are posted\n"
            "**📅 Set Inactive Days:** Set the threshold for /inactive command\n"
            "**🔄 Update All Members:** Scan and update all current members in the database\n"
            "**⚙️ View Config:** View current configuration settings"
        )

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    @app_commands.command(name="health", description="Check bot health status (Admin only)")
    async def health(self, interaction: discord.Interaction):
        """
        Display bot health and status information.

        Args:
            interaction: Discord interaction
        """
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{self.config.bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
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

    @app_commands.command(name="help", description="Show bot information and available commands (Admin only)")
    async def help(self, interaction: discord.Interaction):
        """
        Display bot information and list of all available commands.

        Args:
            interaction: Discord interaction
        """
        # Check permissions
        if not has_bot_admin_role(interaction.user, self.config.bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{self.config.bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
            return

        # Create help embed
        embed = create_embed("LastSeen Bot - Help", discord.Color.blue())
        embed.description = (
            "A Discord bot for tracking user activity across your server. "
            "Monitors when members join, leave, go offline/online, and tracks nickname and role changes."
        )

        # User Commands
        embed.add_field(
            name="👥 User Commands",
            value=(
                "**`/whois <user>`**\n"
                "Display detailed information about a user including their username, nickname, roles, join date, and last seen time.\n\n"
                "**`/lastseen <user>`** or **`/seen <user>`**\n"
                "Check when a specific user was last seen online. Shows 'Currently online' if they're active.\n\n"
                "**`/inactive`**\n"
                "List all members who have been inactive (offline) for longer than the configured threshold. "
                "Results are paginated with interactive navigation buttons."
            ),
            inline=False
        )

        # Admin Commands
        embed.add_field(
            name="⚙️ Admin Commands",
            value=(
                "**`/config`**\n"
                "Open an interactive configuration panel with buttons to:\n"
                "• Set the notification channel for member leave messages\n"
                "• Configure the inactive days threshold\n"
                "• Update all members in the database\n"
                "• View current configuration\n\n"
                "**`/health`**\n"
                "Check the bot's health status including uptime, latency, database health, and guild-specific statistics.\n\n"
                "**`/help`**\n"
                "Display this help message with information about all available commands."
            ),
            inline=False
        )

        # Key Features
        embed.add_field(
            name="✨ Key Features",
            value=(
                "• **Presence Tracking** - Records when users go offline and come back online\n"
                "• **Member History** - Tracks all members who have ever been in the server\n"
                "• **Rejoining Members** - Automatically updates records when users rejoin\n"
                "• **Privacy Focused** - All data stored locally, no external services"
            ),
            inline=False
        )

        # Footer with helpful info
        embed.set_footer(text=f"Bot Admin Role: {self.config.bot_admin_role_name} | Commands shown are admin-restricted")

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")


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

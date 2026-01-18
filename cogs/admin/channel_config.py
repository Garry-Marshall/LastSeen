"""Channel and timing configuration modals."""

import discord
import logging

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed

logger = logging.getLogger(__name__)


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

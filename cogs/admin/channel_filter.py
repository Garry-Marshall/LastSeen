"""Channel-based command restriction modal."""

import discord
import logging

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed

logger = logging.getLogger(__name__)


class AllowedChannelsModal(discord.ui.Modal, title="Set Allowed Channels"):
    """Modal for setting which channels can use commands (optional)."""

    channels_input = discord.ui.TextInput(
        label="Channel Names or IDs (comma-separated)",
        placeholder="e.g., general, bot-commands, 123456 (leave empty for all)",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500
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
        channels_str = self.channels_input.value.strip()

        # If empty, allow all channels
        if not channels_str:
            if self.db.set_allowed_channels(self.guild_id, [], interaction.guild.name):
                await interaction.response.send_message(
                    embed=create_success_embed("Bot commands can now be used in all channels"),
                    ephemeral=True
                )
                logger.info(f"Allowed channels cleared in guild {interaction.guild.name}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed("Failed to update allowed channels."),
                    ephemeral=True
                )
            return

        # Parse comma-separated channel names/IDs
        channel_identifiers = [c.strip() for c in channels_str.split(',') if c.strip()]

        if not channel_identifiers:
            await interaction.response.send_message(
                embed=create_error_embed("No valid channel identifiers provided."),
                ephemeral=True
            )
            return

        # Resolve channels
        channel_ids = []
        missing_channels = []

        for identifier in channel_identifiers:
            channel = None

            # Try as ID
            try:
                channel_id = int(identifier)
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                # Try as name (remove # if present)
                search_name = identifier.lstrip('#').lower()
                channel = discord.utils.get(interaction.guild.text_channels, name=search_name)

            if channel:
                channel_ids.append(channel.id)
            else:
                missing_channels.append(identifier)

        if not channel_ids:
            await interaction.response.send_message(
                embed=create_error_embed("None of the provided channels could be found."),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_allowed_channels(self.guild_id, channel_ids, interaction.guild.name):
            channels = [interaction.guild.get_channel(cid) for cid in channel_ids]
            channel_mentions = [ch.mention for ch in channels if ch]

            message = f"Bot commands can now only be used in: {', '.join(channel_mentions)}"
            if missing_channels:
                message += f"\n\n⚠️ Warning: These channels weren't found: {', '.join(missing_channels)}"

            await interaction.response.send_message(
                embed=create_success_embed(message),
                ephemeral=True
            )
            logger.info(f"Allowed channels set to {channel_ids} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update allowed channels."),
                ephemeral=True
            )

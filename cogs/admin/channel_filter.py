"""Channel-based command restriction modal."""

import discord
import logging

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed
from bot.locale import t, guild_language

logger = logging.getLogger(__name__)


class AllowedChannelsModal(discord.ui.Modal):
    """Modal for setting which channels can use commands (optional)."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        self.lang = guild_language(db.get_guild_config(guild_id))
        super().__init__(title=t("admin.allowed_channels.modal_title", self.lang))

        self.channels_input = discord.ui.TextInput(
            label=t("admin.allowed_channels.input_label", self.lang),
            placeholder=t("admin.allowed_channels.input_placeholder", self.lang),
            required=False,
            style=discord.TextStyle.paragraph,
            max_length=500
        )
        self.add_item(self.channels_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
        channels_str = self.channels_input.value.strip()

        # If empty, allow all channels
        if not channels_str:
            if self.db.set_allowed_channels(self.guild_id, [], interaction.guild.name):
                await interaction.response.send_message(
                    embed=create_success_embed(t("admin.allowed_channels.cleared", lang), lang),
                    ephemeral=True
                )
                logger.info(f"Allowed channels cleared in guild {interaction.guild.name}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed(t("admin.allowed_channels.update_failed", lang), lang),
                    ephemeral=True
                )
            return

        # Parse comma-separated channel names/IDs
        channel_identifiers = [c.strip() for c in channels_str.split(',') if c.strip()]

        if not channel_identifiers:
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.allowed_channels.no_identifiers", lang), lang),
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
                embed=create_error_embed(t("admin.allowed_channels.none_found", lang), lang),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_allowed_channels(self.guild_id, channel_ids, interaction.guild.name):
            channels = [interaction.guild.get_channel(cid) for cid in channel_ids]
            channel_mentions = [ch.mention for ch in channels if ch]

            message = t("admin.allowed_channels.set", lang, channels=', '.join(channel_mentions))
            if missing_channels:
                message += t("admin.allowed_channels.set_warning", lang, channels=', '.join(missing_channels))

            await interaction.response.send_message(
                embed=create_success_embed(message, lang),
                ephemeral=True
            )
            logger.info(f"Allowed channels set to {channel_ids} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.allowed_channels.update_failed", lang), lang),
                ephemeral=True
            )

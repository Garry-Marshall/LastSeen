"""Channel-based command restriction modal."""

import asyncio
import discord
import json
import logging

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed
from bot.locale import t, guild_language

logger = logging.getLogger(__name__)


class AllowedChannelsModal(discord.ui.Modal):
    """Modal for setting which channels can use commands (optional)."""

    def __init__(self, db: DatabaseManager, guild_id: int, guild_config: dict | None,
                 guild: discord.Guild | None = None):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
            guild_config: The guild's config row, read by the caller off the event loop
            guild: The guild, to show the current channels by name
        """
        self.db = db
        self.guild_id = guild_id
        self.lang = guild_language(guild_config)
        super().__init__(title=t("admin.allowed_channels.modal_title", self.lang))

        self.add_item(discord.ui.TextDisplay(t("admin.allowed_channels.info", self.lang)))

        self.channels_input = discord.ui.TextInput(
            placeholder=t("admin.allowed_channels.input_placeholder", self.lang),
            required=False,
            style=discord.TextStyle.paragraph,
            # Discord's maximum: a prefill longer than max_length would stop
            # the dialog from opening at all
            max_length=4000
        )
        # Pre-fill with the current setting so it can be reviewed or edited
        current = self._current_channels_text(guild_config, guild)
        if current:
            self.channels_input.default = current
        self.add_item(discord.ui.Label(
            text=t("admin.allowed_channels.input_label", self.lang),
            component=self.channels_input
        ))

    @staticmethod
    def _current_channels_text(guild_config: dict | None, guild: discord.Guild | None) -> str:
        """The stored allowed channels as text that on_submit reads back to the
        same channels: '#name' for a text channel with a unique name (on_submit
        looks text channels up by name), the ID otherwise (other channel types,
        duplicate names, deleted channels)."""
        try:
            ids = json.loads(guild_config.get('allowed_channels') or '[]') if guild_config else []
        except (json.JSONDecodeError, TypeError):
            return ''
        text_names = [c.name for c in guild.text_channels] if guild else []
        parts = []
        for channel_id in ids:
            channel = guild.get_channel(channel_id) if guild else None
            if isinstance(channel, discord.TextChannel) and text_names.count(channel.name) == 1:
                parts.append(f"#{channel.name}")
            else:
                parts.append(str(channel_id))
        return ', '.join(parts)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
        channels_str = self.channels_input.value.strip()

        # If empty, allow all channels
        if not channels_str:
            if await asyncio.to_thread(self.db.set_allowed_channels, self.guild_id, [], interaction.guild.name):
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
        if await asyncio.to_thread(self.db.set_allowed_channels, self.guild_id, channel_ids, interaction.guild.name):
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

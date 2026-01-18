"""Bot client setup and initialization."""

import discord
from discord.ext import commands
import logging
from pathlib import Path

from database import DatabaseManager

logger = logging.getLogger(__name__)


def create_bot(config) -> commands.Bot:
    """
    Create and configure the Discord bot.

    Args:
        config: Bot configuration

    Returns:
        Configured bot instance
    """
    # Set up intents
    intents = discord.Intents.default()
    intents.members = True  # Required for member events
    intents.presences = True  # Required for presence tracking
    #intents.message_content = True  # Required for reading message content

    # Create bot instance
    bot = commands.Bot(
        command_prefix='!',  # Prefix for legacy commands (not used for slash commands)
        intents=intents,
        help_command=None  # Disable default help command
    )

    # Attach configuration and database to bot
    bot.config = config
    bot.db = DatabaseManager(config.db_file)
    bot.start_time = None  # Will be set in on_ready

    @bot.event
    async def on_ready():
        """Called when the bot is fully ready."""
        # Set start time on first ready event
        if bot.start_time is None:
            from datetime import datetime, timezone
            bot.start_time = datetime.now(timezone.utc)

        logger.info(f"Logged in as {bot.user} (ID: {bot.user.id})")
        logger.info(f"Connected to {len(bot.guilds)} guilds")

        # Enumerate all guilds
        for guild in bot.guilds:
            logger.info(f"  - {guild.name} (ID: {guild.id}, Members: {guild.member_count})")

        # Sync commands with Discord (must be done after bot is ready)
        try:
            synced = await bot.tree.sync()
            logger.info(f"Synced {len(synced)} command(s) with Discord")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}", exc_info=True)

        logger.info("Bot is ready!")

    @bot.event
    async def on_error(event: str, *args, **kwargs):
        """Global error handler for events."""
        logger.error(f"Error in event {event}", exc_info=True)

    @bot.tree.error
    async def on_app_command_error(
        interaction: discord.Interaction,
        error: discord.app_commands.AppCommandError
    ):
        """Global error handler for application commands."""
        # Handle cooldown errors
        if isinstance(error, discord.app_commands.CommandOnCooldown):
            logger.info(f"User {interaction.user} hit cooldown on {interaction.command.name if interaction.command else 'unknown'}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(
                        f"This command is on cooldown. Please wait {error.retry_after:.1f} seconds.",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        f"This command is on cooldown. Please wait {error.retry_after:.1f} seconds.",
                        ephemeral=True
                    )
            except Exception as e:
                logger.error(f"Failed to send cooldown message: {e}")
            return

        # Log other errors
        logger.error(f"Error in command {interaction.command.name if interaction.command else 'unknown'}: {error}", exc_info=True)

        # Send generic error message to user (don't expose internal details)
        try:
            if interaction.response.is_done():
                await interaction.followup.send(
                    "An unexpected error occurred. Please try again later.",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "An unexpected error occurred. Please try again later.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")

    return bot


async def load_cogs(bot: commands.Bot):
    """
    Load all cogs for the bot.

    Args:
        bot: Bot instance
    """
    cogs = [
        'cogs.tracking',
        'cogs.commands',
        'cogs.admin'
    ]

    for cog in cogs:
        try:
            await bot.load_extension(cog)
            logger.info(f"Loaded cog: {cog}")
        except Exception as e:
            logger.error(f"Failed to load cog {cog}: {e}", exc_info=True)


async def setup_bot(bot: commands.Bot):
    """
    Set up the bot by loading cogs.

    Args:
        bot: Bot instance
    """
    # Load all cogs
    await load_cogs(bot)

    # Note: Command syncing is done in on_ready event after bot connects

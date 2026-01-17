"""
LastSeen Discord Bot - Main Entry Point

A Discord bot for monitoring and tracking user activity across guilds.
Tracks user joins, leaves, nickname changes, and presence updates.
"""

import asyncio
import logging
import sys
from pathlib import Path

from bot.config import Config
from bot.client import create_bot, setup_bot

# Setup basic logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the bot."""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = Config()

        # Setup logging with config settings
        config.setup_logging()
        logger.info("Configuration loaded successfully")

        # Create bot instance
        logger.info("Creating bot instance...")
        bot = create_bot(config)

        # Setup bot (load cogs)
        logger.info("Setting up bot...")
        async with bot:
            await setup_bot(bot)

            # Start the bot
            logger.info("Starting bot...")
            await bot.start(config.bot_token)

    except FileNotFoundError as e:
        logger.error("Configuration error:")
        logger.error(str(e))
        logger.error("\nPlease follow these steps:")
        logger.error("1. Edit the .env file")
        logger.error("2. Add your Discord bot token")
        logger.error("3. Restart the bot")
        sys.exit(1)

    except ValueError as e:
        logger.error("Configuration error:")
        logger.error(str(e))
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Bot shutdown requested by user")
        sys.exit(0)

    except Exception as e:
        logger.error("Fatal error occurred:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    """Run the bot."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")

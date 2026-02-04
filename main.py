"""
LastSeen Discord Bot - Main Entry Point

A Discord bot for monitoring and tracking user activity across guilds.
Tracks user joins, leaves, nickname changes, and presence updates.
"""

import asyncio
import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from bot.config import Config
from bot.client import create_bot, setup_bot

# Exit codes
EXIT_OK = 0
EXIT_CRASH = 1       # Unexpected error — restart loop will retry
EXIT_CONFIG = 2      # Configuration error — do not restart

# Setup basic logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def write_crash_log(exit_code):
    """Fallback: write crash details to logs/crash.log when the bot exits abnormally.

    This runs regardless of whether the rotating log handler was set up,
    so crashes during early startup are not lost.
    """
    if exit_code == EXIT_OK:
        return

    try:
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        exc_info = traceback.format_exc()
        is_traceback = exc_info and exc_info.strip() != 'NoneType: None'

        with open(log_dir / 'crash.log', 'a', encoding='utf-8') as f:
            f.write(f"\n{'=' * 60}\n")
            f.write(f"Crash at {timestamp} (exit code {exit_code})\n")
            f.write(f"{'=' * 60}\n")
            if is_traceback:
                f.write(exc_info)
            else:
                f.write("No traceback available.\n")
            f.write("\n")
    except Exception:
        pass  # Nothing we can do if we can't even write the crash log


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
        sys.exit(EXIT_CONFIG)

    except ValueError as e:
        logger.error("Configuration error:")
        logger.error(str(e))
        sys.exit(EXIT_CONFIG)

    except KeyboardInterrupt:
        logger.info("Bot shutdown requested by user")
        sys.exit(EXIT_OK)

    except Exception as e:
        logger.error("Fatal error occurred:", exc_info=True)
        sys.exit(EXIT_CRASH)


if __name__ == "__main__":
    """Run the bot."""
    exit_code = EXIT_OK
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except SystemExit as e:
        exit_code = e.code if e.code is not None else EXIT_OK
    finally:
        write_crash_log(exit_code)
        sys.exit(exit_code)

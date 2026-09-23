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

import discord

from bot.config import Config
from bot.client import create_bot, setup_bot

# Exit codes
EXIT_OK = 0
EXIT_CRASH = 1       # Unexpected error — restart loop will retry
EXIT_CONFIG = 2      # Configuration error — do not restart

# Wait this long before exiting when Discord refuses the privileged intents
# (e.g. the yearly intent re-evaluation is delayed). The restart loop then
# retries about 144 times a day: well under Discord's 1000 logins (IDENTIFY)
# per 24h, past which Discord resets the bot token. Without the wait it would
# retry every few seconds and hit that limit within hours.
INTENTS_RETRY_DELAY_SECONDS = 600

# Setup basic logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def write_crash_log(exit_code, details=None):
    """Fallback: write crash details to logs/crash.log when the bot exits abnormally.

    This runs regardless of whether the rotating log handler was set up,
    so crashes during early startup are not lost.

    details: the formatted traceback, captured by the caller while the
    exception was still being handled (afterwards it is gone).
    """
    if exit_code == EXIT_OK:
        return

    try:
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        with open(log_dir / 'crash.log', 'a', encoding='utf-8') as f:
            f.write(f"\n{'=' * 60}\n")
            f.write(f"Crash at {timestamp} (exit code {exit_code})\n")
            f.write(f"{'=' * 60}\n")
            f.write(details if details else "No traceback available.\n")
            f.write("\n")
    except Exception:
        pass  # Nothing we can do if we can't even write the crash log


async def main():
    """Main entry point for the bot.

    Only a problem loading the configuration, or Discord rejecting the bot
    token, exits with EXIT_CONFIG (which start_bot.bat does not restart).
    Any other error is a crash: it propagates to __main__, which logs it,
    records the traceback in crash.log and exits with EXIT_CRASH so the bot
    is restarted. Discord refusing the privileged intents is a crash too, but
    only after waiting INTENTS_RETRY_DELAY_SECONDS.
    """
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = Config()

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

    # Setup logging with config settings
    config.setup_logging()
    logger.info("Configuration loaded successfully")

    # Create bot instance
    logger.info("Creating bot instance...")
    bot = create_bot(config)

    # Setup bot (load cogs)
    logger.info("Setting up bot...")
    try:
        async with bot:
            await setup_bot(bot)

            # Start the bot
            logger.info("Starting bot...")
            await bot.start(config.bot_token)

    except discord.LoginFailure as e:
        # A rejected token is a configuration problem, not a crash: retrying it
        # every few seconds can't succeed and repeated failed logins can get
        # the bot's IP rate-limited by Discord.
        logger.error("Configuration error: Discord rejected the bot token.")
        logger.error(str(e))
        logger.error("Check DISCORD_BOT_TOKEN in the .env file, then restart the bot.")
        sys.exit(EXIT_CONFIG)

    except discord.PrivilegedIntentsRequired:
        # Not a config exit: the intents can come back (e.g. after a delayed
        # re-evaluation), and the bot should then return by itself. Retry, but
        # slowly — see INTENTS_RETRY_DELAY_SECONDS.
        logger.error("Discord refused the privileged intents (Presences / Server Members). "
                     "Check the Developer Portal and the intent review status.")
        logger.error(f"Retrying in {INTENTS_RETRY_DELAY_SECONDS // 60} minutes...")
        await asyncio.sleep(INTENTS_RETRY_DELAY_SECONDS)
        raise  # a crash: logged with its traceback, exit code 1, restarted


if __name__ == "__main__":
    """Run the bot."""
    exit_code = EXIT_OK
    crash_details = None
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except SystemExit as e:
        exit_code = e.code if e.code is not None else EXIT_OK
    except BaseException:
        # Any other error escaping main() is a crash — including BaseException
        # subclasses such as CancelledError that `except Exception` misses, which
        # used to fall through to exit code 0 so start_bot.bat didn't restart.
        # The traceback is captured here, while it still exists.
        logger.error("Fatal error occurred:", exc_info=True)
        crash_details = traceback.format_exc()
        exit_code = EXIT_CRASH
    finally:
        write_crash_log(exit_code, crash_details)
        sys.exit(exit_code)

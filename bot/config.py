"""Configuration loader for LastSeen bot."""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Config:
    """Bot configuration loaded from .env file."""

    def __init__(self):
        """Load configuration from .env file."""
        self.env_path = Path('.env')
        self.template_path = Path('.env.template')

        # Check if .env exists
        if not self.env_path.exists():
            self._create_default_env()
            logger.error("No .env file found. A default .env file has been created.")
            logger.error("Please edit .env and add your Discord bot token, then restart the bot.")
            raise FileNotFoundError(
                "Please edit .env file and add your Discord bot token, then restart the bot."
            )

        # Load environment variables
        load_dotenv(self.env_path)

        # Load required settings
        self.bot_token = os.getenv('DISCORD_BOT_TOKEN', '').strip()
        if not self.bot_token or self.bot_token == 'your-bot-token-here':
            logger.error("DISCORD_BOT_TOKEN not set in .env file.")
            raise ValueError(
                "Please set DISCORD_BOT_TOKEN in .env file with your actual bot token."
            )

        # Load database settings
        self.db_file = os.getenv('DB_FILE', 'lastseen_bot.db').strip()

        # Load logging settings
        debug_level = os.getenv('DEBUG_LEVEL', 'info').strip().upper()
        self.log_level = getattr(logging, debug_level, logging.INFO)

        # Load bot admin settings
        self.bot_admin_role_name = os.getenv('BOT_ADMIN_ROLE_NAME', 'Bot Admin').strip()

        # Load default settings
        try:
            self.default_inactive_days = int(os.getenv('DEFAULT_INACTIVE_DAYS', '10'))
        except ValueError:
            self.default_inactive_days = 10
            logger.warning("Invalid DEFAULT_INACTIVE_DAYS, using 10")

        # Load log retention settings
        try:
            self.logs_days_to_keep = int(os.getenv('DEBUG_LOGS_DAYS_TO_KEEP', '5'))
        except ValueError:
            self.logs_days_to_keep = 5
            logger.warning("Invalid DEBUG_LOGS_DAYS_TO_KEEP, using 5")

        # Load backup settings
        try:
            self.backup_interval_hours = int(os.getenv('DB_BACKUP_INTERVAL_HOURS', '24'))
            if self.backup_interval_hours < 1:
                self.backup_interval_hours = 24
                logger.warning("DB_BACKUP_INTERVAL_HOURS must be at least 1, using 24")
        except ValueError:
            self.backup_interval_hours = 24
            logger.warning("Invalid DB_BACKUP_INTERVAL_HOURS, using 24")

        try:
            self.backup_retention_count = int(os.getenv('DB_BACKUP_RETENTION_COUNT', '5'))
            if self.backup_retention_count < 1:
                self.backup_retention_count = 5
                logger.warning("DB_BACKUP_RETENTION_COUNT must be at least 1, using 5")
        except ValueError:
            self.backup_retention_count = 5
            logger.warning("Invalid DB_BACKUP_RETENTION_COUNT, using 5")

        logger.info("Configuration loaded successfully")
        logger.info(f"Database file: {self.db_file}")
        logger.info(f"Log level: {logging.getLevelName(self.log_level)}")
        logger.info(f"Bot admin role: {self.bot_admin_role_name}")
        logger.info(f"Log retention: {self.logs_days_to_keep} days")
        logger.info(f"Database backup: every {self.backup_interval_hours} hours, keep {self.backup_retention_count} backups")

    def _create_default_env(self):
        """Create a default .env file from template if it doesn't exist."""
        try:
            if self.template_path.exists():
                # Copy template to .env
                template_content = self.template_path.read_text()
                self.env_path.write_text(template_content)
                logger.info("Created .env file from template")
            else:
                # Create basic .env file
                default_content = """# Discord Bot Configuration
# Fill in your bot token below

# REQUIRED: Your Discord bot token from https://discord.com/developers/applications
DISCORD_BOT_TOKEN=your-bot-token-here

# REQUIRED: database name
DB_FILE=lastseen_bot.db

# Logging and Debug Settings
DEBUG_LEVEL=info  # options: info, debug, warning, error
DEBUG_LOGS_DAYS_TO_KEEP=5  # number of days to keep log files (older logs are deleted)

# Database Backup Settings
DB_BACKUP_INTERVAL_HOURS=24  # how often to backup the database (in hours)
DB_BACKUP_RETENTION_COUNT=5  # number of backup copies to keep (older backups are deleted)

"""
                self.env_path.write_text(default_content)
                logger.info("Created default .env file")
        except Exception as e:
            logger.error(f"Failed to create .env file: {e}")
            raise

    @property
    def log_folder(self) -> Path:
        """Get the logs folder path."""
        return Path('logs')

    @property
    def backup_folder(self) -> Path:
        """Get the database backup folder path."""
        return Path('backups')

    def cleanup_old_logs(self):
        """Delete log files older than the configured retention period."""
        from datetime import datetime, timedelta, timezone

        if not self.log_folder.exists():
            return

        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.logs_days_to_keep)
            deleted_count = 0

            for log_file in self.log_folder.glob('*.log'):
                try:
                    # Parse date from filename (YYYY-MM-DD.log)
                    file_date_str = log_file.stem  # Gets filename without extension
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')

                    # Delete if older than retention period
                    if file_date < cutoff_date:
                        log_file.unlink()
                        deleted_count += 1
                        logger.info(f"Deleted old log file: {log_file.name}")
                except ValueError:
                    # Skip files that don't match date format
                    logger.debug(f"Skipping non-date log file: {log_file.name}")
                except Exception as e:
                    logger.warning(f"Failed to delete log file {log_file.name}: {e}")

            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old log file(s)")
        except Exception as e:
            logger.error(f"Error during log cleanup: {e}")

    def setup_logging(self):
        """Configure logging for the bot."""
        # Create logs directory if it doesn't exist
        self.log_folder.mkdir(exist_ok=True)

        # Cleanup old log files
        self.cleanup_old_logs()

        # Get current date for log file base name
        from datetime import datetime, timezone
        log_date = datetime.now().strftime('%Y-%m-%d')
        log_file = self.log_folder / f"{log_date}.log"

        # Configure logging format
        log_format = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'

        # Clear any existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        # Set root logger level
        root_logger.setLevel(self.log_level)

        # File handler with automatic daily rotation
        file_handler = TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=self.logs_days_to_keep,
            encoding='utf-8',
            utc=True
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        # Set suffix for rotated files to match our naming convention
        file_handler.suffix = '%Y-%m-%d.log'
        root_logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(console_handler)

        # Suppress debug messages from discord.py and other noisy libraries
        # Even when bot is in DEBUG mode, keep these at INFO level
        logging.getLogger('discord').setLevel(logging.INFO)
        logging.getLogger('discord.client').setLevel(logging.INFO)
        logging.getLogger('discord.gateway').setLevel(logging.INFO)
        logging.getLogger('discord.http').setLevel(logging.INFO)
        logging.getLogger('discord.state').setLevel(logging.INFO)
        logging.getLogger('discord.webhook').setLevel(logging.INFO)

        logger.info(f"Logging configured: {log_file}")

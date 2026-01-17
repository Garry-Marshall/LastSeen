"""Configuration loader for LastSeen bot."""

import os
import logging
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

        logger.info("Configuration loaded successfully")
        logger.info(f"Database file: {self.db_file}")
        logger.info(f"Log level: {logging.getLevelName(self.log_level)}")
        logger.info(f"Bot admin role: {self.bot_admin_role_name}")

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

# Default bot admin role name
BOT_ADMIN_ROLE_NAME=Bot Admin

# Default Settings
DEFAULT_INACTIVE_DAYS=10
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

    def setup_logging(self):
        """Configure logging for the bot."""
        # Create logs directory if it doesn't exist
        self.log_folder.mkdir(exist_ok=True)

        # Get current date for log file name
        from datetime import datetime
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

        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(console_handler)

        logger.info(f"Logging configured: {log_file}")

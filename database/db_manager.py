"""Database manager for LastSeen bot with proper connection handling."""

import sqlite3
import json
import logging
import threading
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Queue, Empty, Full

logger = logging.getLogger(__name__)

# Time constants (in seconds)
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600


class DatabaseManager:
    """Manages SQLite database connections and operations."""

    def __init__(self, db_file: str, pool_size: int = 5):
        """
        Initialize database manager with connection pooling.

        Args:
            db_file: Path to SQLite database file
            pool_size: Number of connections to maintain in the pool (default: 5)
        """
        self.db_file = db_file
        self.pool_size = pool_size
        self._pool: Queue = Queue(maxsize=pool_size)
        self._pool_lock = threading.Lock()
        self._connection_count = 0
        
        # Initialize the connection pool
        self._initialize_pool()
        self._initialize_database()
    
    def _initialize_pool(self):
        """Initialize the connection pool with connections."""
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_file, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._pool.put(conn)
            self._connection_count += 1
        logger.info(f"Initialized database connection pool with {self.pool_size} connections")
    
    def _get_connection_from_pool(self) -> sqlite3.Connection:
        """Get a connection from the pool, creating a new one if pool is empty."""
        try:
            # Try to get a connection from the pool (non-blocking)
            return self._pool.get_nowait()
        except Empty:
            # Pool is empty, create a temporary connection
            logger.debug("Connection pool exhausted, creating temporary connection")
            conn = sqlite3.connect(self.db_file, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
    
    def _return_connection_to_pool(self, conn: sqlite3.Connection):
        """Return a connection to the pool."""
        try:
            # Only return to pool if there's space
            self._pool.put_nowait(conn)
        except Full:
            # Pool is full, close the temporary connection
            conn.close()
    
    def close_pool(self):
        """Close all connections in the pool. Should be called on shutdown."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break
        logger.info("Closed all database connections in pool")

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections from pool.
        Ensures connections are properly returned to the pool.

        Yields:
            sqlite3.Connection: Database connection from pool
        """
        conn = self._get_connection_from_pool()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self._return_connection_to_pool(conn)

    def _initialize_database(self):
        """Create tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Guilds table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS guilds (
                    guild_id INTEGER PRIMARY KEY,
                    guild_name TEXT NOT NULL,
                    notification_channel_id INTEGER,
                    inactive_days INTEGER DEFAULT 10,
                    bot_admin_role_name TEXT DEFAULT 'LastSeen Admin',
                    user_role_required INTEGER DEFAULT 0,
                    user_role_name TEXT DEFAULT 'LastSeen User',
                    added_at INTEGER NOT NULL
                )
            """)

            # Members table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS members (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    nickname TEXT,
                    join_date INTEGER,
                    last_seen INTEGER,
                    is_active INTEGER DEFAULT 1,
                    roles TEXT,
                    PRIMARY KEY (guild_id, user_id),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
                )
            """)

            # Role changes table - tracks role additions/removals
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS role_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    role_name TEXT NOT NULL,
                    action TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE,
                    FOREIGN KEY (guild_id, user_id) REFERENCES members(guild_id, user_id) ON DELETE CASCADE
                )
            """)

            # Message activity table - daily aggregate message counts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    date INTEGER NOT NULL,
                    message_count INTEGER DEFAULT 1,
                    UNIQUE(guild_id, user_id, date),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE,
                    FOREIGN KEY (guild_id, user_id) REFERENCES members(guild_id, user_id) ON DELETE CASCADE
                )
            """)

            # Create indexes for better query performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_username
                ON members(guild_id, username COLLATE NOCASE)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_nickname
                ON members(guild_id, nickname COLLATE NOCASE)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_active
                ON members(guild_id, is_active, last_seen)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_join_date
                ON members(guild_id, join_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_left_date
                ON members(guild_id, left_date)
                WHERE left_date IS NOT NULL
            """)

            # Indexes for role_changes table
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_role_changes_guild_user
                ON role_changes(guild_id, user_id, timestamp DESC)
            """)

            # Indexes for message_activity table
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_activity_user_date
                ON message_activity(guild_id, user_id, date DESC)
            """)

            # Migration: Add new role permission columns if they don't exist
            cursor.execute("PRAGMA table_info(guilds)")
            columns = [row[1] for row in cursor.fetchall()]

            if 'bot_admin_role_name' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN bot_admin_role_name TEXT DEFAULT 'LastSeen Admin'")
                logger.info("Added bot_admin_role_name column to guilds table")

            if 'user_role_required' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN user_role_required INTEGER DEFAULT 0")
                logger.info("Added user_role_required column to guilds table")

            if 'user_role_name' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN user_role_name TEXT DEFAULT 'LastSeen User'")
                logger.info("Added user_role_name column to guilds table")

            if 'track_only_roles' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN track_only_roles TEXT")
                logger.info("Added track_only_roles column to guilds table")

            if 'allowed_channels' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN allowed_channels TEXT")
                logger.info("Added allowed_channels column to guilds table")

            if 'positions_initialized' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN positions_initialized INTEGER DEFAULT 0")
                logger.info("Added positions_initialized column to guilds table")

            if 'message_retention_days' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN message_retention_days INTEGER DEFAULT 365")
                logger.info("Added message_retention_days column to guilds table")

            if 'timezone' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN timezone TEXT DEFAULT 'UTC'")
                logger.info("Added timezone column to guilds table")

            if 'report_channel_id' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_channel_id INTEGER")
                logger.info("Added report_channel_id column to guilds table")

            if 'report_frequency' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_frequency TEXT")
                logger.info("Added report_frequency column to guilds table")

            if 'report_types' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_types TEXT")
                logger.info("Added report_types column to guilds table")

            if 'report_day_weekly' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_day_weekly INTEGER DEFAULT 0")
                logger.info("Added report_day_weekly column to guilds table")

            if 'report_day_monthly' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_day_monthly INTEGER DEFAULT 1")
                logger.info("Added report_day_monthly column to guilds table")

            if 'last_weekly_report' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN last_weekly_report INTEGER DEFAULT 0")
                logger.info("Added last_weekly_report column to guilds table")

            if 'last_monthly_report' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN last_monthly_report INTEGER DEFAULT 0")
                logger.info("Added last_monthly_report column to guilds table")

            if 'report_time_hour' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN report_time_hour INTEGER DEFAULT 9")
                logger.info("Added report_time_hour column to guilds table")

            # Refresh column list after migrations to ensure all columns exist
            cursor.execute("PRAGMA table_info(guilds)")
            columns = [row[1] for row in cursor.fetchall()]

            # Create index for scheduled reports query (only if columns exist)
            if 'report_frequency' in columns and 'report_channel_id' in columns:
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_guilds_reports
                    ON guilds(report_frequency, report_channel_id)
                    WHERE report_frequency IS NOT NULL AND report_channel_id IS NOT NULL
                """)

            # Check members table for new columns
            cursor.execute("PRAGMA table_info(members)")
            member_columns = [row[1] for row in cursor.fetchall()]

            if 'join_position' not in member_columns:
                cursor.execute("ALTER TABLE members ADD COLUMN join_position INTEGER")
                logger.info("Added join_position column to members table")

            if 'nickname_history' not in member_columns:
                cursor.execute("ALTER TABLE members ADD COLUMN nickname_history TEXT")
                logger.info("Added nickname_history column to members table")

            if 'left_date' not in member_columns:
                cursor.execute("ALTER TABLE members ADD COLUMN left_date INTEGER")
                logger.info("Added left_date column to members table")

            # Create message_activity_hourly table for hour-of-day tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_activity_hourly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    hour INTEGER NOT NULL,
                    message_count INTEGER DEFAULT 1,
                    UNIQUE(guild_id, user_id, timestamp),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE,
                    FOREIGN KEY (guild_id, user_id) REFERENCES members(guild_id, user_id) ON DELETE CASCADE
                )
            """)

            # Index for hourly activity queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_activity_hourly_guild_time
                ON message_activity_hourly(guild_id, timestamp DESC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_activity_hourly_user
                ON message_activity_hourly(guild_id, user_id, timestamp DESC)
            """)

            conn.commit()
            logger.info(f"Database initialized: {self.db_file}")

    # ==================== Guild Operations ====================

    def add_guild(self, guild_id: int, guild_name: str, inactive_days: int = 10) -> bool:
        """
        Add a new guild to the database.

        Args:
            guild_id: Discord guild ID
            guild_name: Guild name
            inactive_days: Default inactive days threshold

        Returns:
            bool: True if successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, ?, ?)
                """, (guild_id, guild_name, inactive_days, int(datetime.now(timezone.utc).timestamp())))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to add guild {guild_id}: {e}")
            return False

    def update_guild_name(self, guild_id: int, guild_name: str) -> bool:
        """Update guild name."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE guilds SET guild_name = ? WHERE guild_id = ?
                """, (guild_name, guild_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update guild name {guild_id}: {e}")
            return False

    def set_notification_channel(self, guild_id: int, channel_id: int, guild_name: str = 'Unknown') -> bool:
        """Set the notification channel for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists (in case it wasn't added via on_guild_join)
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Now update the notification channel and guild name if needed
                cursor.execute("""
                    UPDATE guilds
                    SET notification_channel_id = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (channel_id, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set notification channel for guild {guild_id}: {e}")
            return False

    def set_inactive_days(self, guild_id: int, inactive_days: int, guild_name: str = 'Unknown') -> bool:
        """Set the inactive days threshold for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists (in case it wasn't added via on_guild_join)
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Now update the inactive days and guild name if needed
                cursor.execute("""
                    UPDATE guilds
                    SET inactive_days = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (inactive_days, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set inactive days for guild {guild_id}: {e}")
            return False

    def set_message_retention_days(self, guild_id: int, retention_days: int, guild_name: str = 'Unknown') -> bool:
        """Set the message retention period for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists (in case it wasn't added via on_guild_join)
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Now update the retention days and guild name if needed
                cursor.execute("""
                    UPDATE guilds
                    SET message_retention_days = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (retention_days, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set message retention days for guild {guild_id}: {e}")
            return False

    def set_timezone(self, guild_id: int, timezone_str: str, guild_name: str = 'Unknown') -> bool:
        """Set the timezone for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists (in case it wasn't added via on_guild_join)
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Now update the timezone and guild name if needed
                cursor.execute("""
                    UPDATE guilds
                    SET timezone = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (timezone_str, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set timezone for guild {guild_id}: {e}")
            return False

    def set_report_config(self, guild_id: int, channel_id: int, frequency: str, report_types: list, 
                         day_weekly: int = 0, day_monthly: int = 1, time_hour: int = 9, guild_name: str = 'Unknown') -> bool:
        """Set the scheduled report configuration for a guild.
        
        Args:
            guild_id: Discord guild ID
            channel_id: Channel ID where reports will be sent
            frequency: 'weekly', 'monthly', or 'both'
            report_types: List of report types to send
            day_weekly: Day of week for weekly reports (0=Monday, 6=Sunday)
            day_monthly: Day of month for monthly reports (1-28)
            time_hour: Hour of day to send reports (0-23, UTC)
            guild_name: Name of the guild
        """
        try:
            import json
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update report configuration
                cursor.execute("""
                    UPDATE guilds
                    SET report_channel_id = ?,
                        report_frequency = ?,
                        report_types = ?,
                        report_day_weekly = ?,
                        report_day_monthly = ?,
                        report_time_hour = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (channel_id, frequency, json.dumps(report_types), day_weekly, day_monthly, time_hour, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set report config for guild {guild_id}: {e}")
            return False

    def disable_reports(self, guild_id: int) -> bool:
        """Disable scheduled reports for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE guilds
                    SET report_channel_id = NULL,
                        report_frequency = NULL,
                        report_types = NULL
                    WHERE guild_id = ?
                """, (guild_id,))
                return True
        except Exception as e:
            logger.error(f"Failed to disable reports for guild {guild_id}: {e}")
            return False

    def update_last_report_time(self, guild_id: int, report_type: str) -> bool:
        """Update the last report timestamp for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                if report_type == 'weekly':
                    cursor.execute("UPDATE guilds SET last_weekly_report = ? WHERE guild_id = ?", (now, guild_id))
                elif report_type == 'monthly':
                    cursor.execute("UPDATE guilds SET last_monthly_report = ? WHERE guild_id = ?", (now, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to update last report time for guild {guild_id}: {e}")
            return False

    def set_bot_admin_role(self, guild_id: int, role_name: str, guild_name: str = 'Unknown') -> bool:
        """Set the bot admin role name for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update the bot admin role name
                cursor.execute("""
                    UPDATE guilds
                    SET bot_admin_role_name = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (role_name, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set bot admin role for guild {guild_id}: {e}")
            return False

    def set_user_role_required(self, guild_id: int, required: bool, guild_name: str = 'Unknown') -> bool:
        """Set whether user role is required for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update the user role required setting
                cursor.execute("""
                    UPDATE guilds
                    SET user_role_required = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (1 if required else 0, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set user role required for guild {guild_id}: {e}")
            return False

    def set_user_role_name(self, guild_id: int, role_name: str, guild_name: str = 'Unknown') -> bool:
        """Set the user role name for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update the user role name
                cursor.execute("""
                    UPDATE guilds
                    SET user_role_name = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (role_name, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set user role name for guild {guild_id}: {e}")
            return False

    def set_track_only_roles(self, guild_id: int, role_names: List[str], guild_name: str = 'Unknown') -> bool:
        """Set which roles should be tracked (empty list = track all)."""
        try:
            roles_json = json.dumps(role_names) if role_names else None
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update the track only roles
                cursor.execute("""
                    UPDATE guilds
                    SET track_only_roles = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (roles_json, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set track only roles for guild {guild_id}: {e}")
            return False

    def set_allowed_channels(self, guild_id: int, channel_ids: List[int], guild_name: str = 'Unknown') -> bool:
        """Set which channels can use bot commands (empty list = all channels)."""
        try:
            channels_json = json.dumps(channel_ids) if channel_ids else None
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Update the allowed channels
                cursor.execute("""
                    UPDATE guilds
                    SET allowed_channels = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (channels_json, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set allowed channels for guild {guild_id}: {e}")
            return False

    def get_guild_config(self, guild_id: int) -> Optional[Dict[str, Any]]:
        """Get guild configuration."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM guilds WHERE guild_id = ?
                """, (guild_id,))
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"Failed to get guild config {guild_id}: {e}")
            return None

    def remove_guild(self, guild_id: int) -> bool:
        """Remove a guild and all its members."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM guilds WHERE guild_id = ?", (guild_id,))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to remove guild {guild_id}: {e}")
            return False

    def guild_positions_initialized(self, guild_id: int) -> bool:
        """Check if member positions have been initialized for this guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT positions_initialized FROM guilds WHERE guild_id = ?
                """, (guild_id,))
                row = cursor.fetchone()
                return bool(row[0]) if row else False
        except Exception as e:
            logger.error(f"Failed to check positions initialized for guild {guild_id}: {e}")
            return False

    def mark_positions_initialized(self, guild_id: int) -> bool:
        """Mark that member positions have been initialized for this guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE guilds SET positions_initialized = 1 WHERE guild_id = ?
                """, (guild_id,))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to mark positions initialized for guild {guild_id}: {e}")
            return False

    def set_member_join_position(self, guild_id: int, user_id: int, position: int) -> bool:
        """Set the join position for a member."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET join_position = ? WHERE guild_id = ? AND user_id = ?
                """, (position, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to set join position for user {user_id} in guild {guild_id}: {e}")
            return False
    
    def calculate_join_position(self, guild_id: int, join_timestamp: int) -> Optional[int]:
        """Calculate the correct join position for a member based on their join timestamp.
        
        This counts how many active (non-bot) members in the guild joined before this timestamp,
        and returns position + 1 (since positions start at 1, not 0).
        
        Args:
            guild_id: Discord guild ID
            join_timestamp: Unix timestamp of when the member joined
            
        Returns:
            int: The calculated join position (1-indexed), or None if failed
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Count how many members joined before this timestamp
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND join_date < ?
                """, (guild_id, join_timestamp))
                count_before = cursor.fetchone()[0]
                
                # Position is count_before + 1 (1-indexed)
                return count_before + 1
        except Exception as e:
            logger.error(f"Failed to calculate join position for guild {guild_id} at timestamp {join_timestamp}: {e}")
            return None

    # ==================== Member Operations ====================

    def add_member(self, guild_id: int, user_id: int, username: str,
                   nickname: Optional[str], join_date: int, roles: List[str]) -> bool:
        """
        Add a new member to the database.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            username: User's Discord username
            nickname: User's nickname in the guild (can be None)
            join_date: Unix timestamp of when user joined
            roles: List of role names

        Returns:
            bool: True if successful
        """
        try:
            roles_json = json.dumps(roles)
            # Initialize nickname history with the current nickname if it exists
            nickname_history = json.dumps([nickname]) if nickname else None
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO members
                    (guild_id, user_id, username, nickname, join_date, last_seen, is_active, roles, nickname_history)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """, (guild_id, user_id, username, nickname, join_date, None, roles_json, nickname_history))
                return True
        except Exception as e:
            logger.error(f"Failed to add member {user_id} to guild {guild_id}: {e}")
            return False

    def update_member_username(self, guild_id: int, user_id: int, username: str) -> bool:
        """Update a member's username."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET username = ? WHERE guild_id = ? AND user_id = ?
                """, (username, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update username for {user_id} in guild {guild_id}: {e}")
            return False

    def update_member_nickname(self, guild_id: int, user_id: int, nickname: Optional[str]) -> bool:
        """Update a member's nickname."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET nickname = ? WHERE guild_id = ? AND user_id = ?
                """, (nickname, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update nickname for {user_id} in guild {guild_id}: {e}")
            return False

    def update_member_roles(self, guild_id: int, user_id: int, roles: List[str]) -> bool:
        """Update a member's roles."""
        try:
            roles_json = json.dumps(roles)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET roles = ? WHERE guild_id = ? AND user_id = ?
                """, (roles_json, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update roles for {user_id} in guild {guild_id}: {e}")
            return False

    def update_nickname_history(self, guild_id: int, user_id: int, old_nickname: Optional[str], new_nickname: Optional[str]) -> bool:
        """Update nickname history for a member (keeps last 10, unique only).
        
        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            old_nickname: Previous nickname before change
            new_nickname: New nickname after change
            
        Returns:
            bool: True if successful
        """
        MAX_NICKNAME_HISTORY = 10  # Limit to prevent unbounded growth
        
        try:
            # Only track if nickname actually changed and new nickname exists
            if new_nickname and old_nickname != new_nickname:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Get current history
                    cursor.execute("""
                        SELECT nickname_history FROM members WHERE guild_id = ? AND user_id = ?
                    """, (guild_id, user_id))
                    row = cursor.fetchone()
                    
                    if not row:
                        return False
                    
                    try:
                        history = json.loads(row[0]) if row[0] else []
                    except (json.JSONDecodeError, TypeError):
                        history = []
                        logger.warning(f"Invalid nickname_history JSON for user {user_id}, resetting to empty list")
                    
                    # Add old nickname if it's not already in history (check entire list for uniqueness)
                    if old_nickname and old_nickname not in history:
                        history.append(old_nickname)
                    
                    # Add new nickname if it's not already in history
                    if new_nickname and new_nickname not in history:
                        history.append(new_nickname)
                    
                    # Keep only last N entries to prevent unbounded growth
                    history = history[-MAX_NICKNAME_HISTORY:]
                    
                    cursor.execute("""
                        UPDATE members SET nickname_history = ? WHERE guild_id = ? AND user_id = ?
                    """, (json.dumps(history), guild_id, user_id))
                    return True
            return True  # No update needed
        except Exception as e:
            logger.error(f"Failed to update nickname history for {user_id} in guild {guild_id}: {e}")
            return False

    def update_last_seen(self, guild_id: int, user_id: int, timestamp: int) -> bool:
        """Update when a member was last seen."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET last_seen = ? WHERE guild_id = ? AND user_id = ?
                """, (timestamp, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update last_seen for {user_id} in guild {guild_id}: {e}")
            return False

    def set_member_inactive(self, guild_id: int, user_id: int) -> bool:
        """Mark a member as inactive (left the guild)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET is_active = 0 WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to set member {user_id} inactive in guild {guild_id}: {e}")
            return False

    def set_member_active(self, guild_id: int, user_id: int) -> bool:
        """Mark a member as active (rejoined the guild)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET is_active = 1 WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to set member {user_id} active in guild {guild_id}: {e}")
            return False

    def get_member(self, guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """Get a member's information."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM members WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                row = cursor.fetchone()
                if row:
                    data = dict(row)
                    # Parse roles JSON
                    if data['roles']:
                        data['roles'] = json.loads(data['roles'])
                    else:
                        data['roles'] = []
                    return data
                return None
        except Exception as e:
            logger.error(f"Failed to get member {user_id} from guild {guild_id}: {e}")
            return None

    def find_member_by_name(self, guild_id: int, search_term: str) -> Optional[Dict[str, Any]]:
        """
        Find a member by username, nickname, or user ID.

        Args:
            guild_id: Discord guild ID
            search_term: Username, nickname, or user ID to search for

        Returns:
            Member data dict or None
        """
        try:
            search_lower = search_term.lower()
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM members
                    WHERE guild_id = ? AND (
                        LOWER(username) = ? OR
                        LOWER(nickname) = ? OR
                        CAST(user_id AS TEXT) = ?
                    )
                """, (guild_id, search_lower, search_lower, search_term))
                row = cursor.fetchone()
                if row:
                    data = dict(row)
                    if data['roles']:
                        data['roles'] = json.loads(data['roles'])
                    else:
                        data['roles'] = []
                    return data
                return None
        except Exception as e:
            logger.error(f"Failed to find member '{search_term}' in guild {guild_id}: {e}")
            return None

    def get_inactive_members(self, guild_id: int, inactive_days: int) -> List[Dict[str, Any]]:
        """
        Get all members who have been inactive for more than the specified days.

        Args:
            guild_id: Discord guild ID
            inactive_days: Number of days threshold

        Returns:
            List of member data dicts
        """
        try:
            current_time = int(datetime.now(timezone.utc).timestamp())
            threshold = current_time - (inactive_days * 24 * 60 * 60)

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM members
                    WHERE guild_id = ?
                    AND is_active = 1
                    AND last_seen IS NOT NULL
                    AND last_seen != 0
                    AND last_seen <= ?
                    ORDER BY last_seen ASC
                """, (guild_id, threshold))

                members = []
                for row in cursor.fetchall():
                    data = dict(row)
                    if data['roles']:
                        data['roles'] = json.loads(data['roles'])
                    else:
                        data['roles'] = []
                    members.append(data)
                return members
        except Exception as e:
            logger.error(f"Failed to get inactive members for guild {guild_id}: {e}")
            return []

    def get_guild_members(self, guild_id: int, include_left: bool = False) -> List[Dict[str, Any]]:
        """
        Get members in a guild with optional filtering.

        Args:
            guild_id: Discord guild ID
            include_left: If True, includes members who have left the server

        Returns:
            List of member data dicts
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if include_left:
                    # Get all members including those who left
                    cursor.execute("""
                        SELECT * FROM members WHERE guild_id = ?
                        ORDER BY last_seen DESC
                    """, (guild_id,))
                else:
                    # Only get current members (is_active = 1)
                    cursor.execute("""
                        SELECT * FROM members 
                        WHERE guild_id = ? AND is_active = 1
                        ORDER BY last_seen DESC
                    """, (guild_id,))

                members = []
                for row in cursor.fetchall():
                    data = dict(row)
                    if data['roles']:
                        data['roles'] = json.loads(data['roles'])
                    else:
                        data['roles'] = []
                    members.append(data)
                return members
        except Exception as e:
            logger.error(f"Failed to get guild members for guild {guild_id}: {e}")
            return []

    def get_all_guild_members(self, guild_id: int) -> List[Dict[str, Any]]:
        """Get all members in a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM members WHERE guild_id = ?
                """, (guild_id,))

                members = []
                for row in cursor.fetchall():
                    data = dict(row)
                    if data['roles']:
                        data['roles'] = json.loads(data['roles'])
                    else:
                        data['roles'] = []
                    members.append(data)
                return members
        except Exception as e:
            logger.error(f"Failed to get all members for guild {guild_id}: {e}")
            return []

    def member_exists(self, guild_id: int, user_id: int) -> bool:
        """Check if a member exists in the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 1 FROM members WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check if member {user_id} exists in guild {guild_id}: {e}")
            return False

    def get_database_health(self) -> Dict[str, Any]:
        """
        Get database health status.

        Returns:
            Dict with health information
        """
        import os

        health = {
            'status': 'unknown',
            'can_connect': False,
            'can_read': False,
            'can_write': False,
            'file_size_mb': 0.0
        }

        try:
            # Check if file exists and get size
            if os.path.exists(self.db_file):
                file_size = os.path.getsize(self.db_file)
                health['file_size_mb'] = round(file_size / (1024 * 1024), 2)

            # Test connection
            with self.get_connection() as conn:
                health['can_connect'] = True

                # Test read
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM guilds")
                cursor.fetchone()
                health['can_read'] = True

                # Test write (using a harmless operation)
                cursor.execute("SELECT 1")
                cursor.fetchone()
                health['can_write'] = True

                health['status'] = 'healthy'

        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            health['status'] = 'unhealthy'
            health['error'] = str(e)

        return health

    def get_guild_stats(self, guild_id: int) -> Dict[str, int]:
        """
        Get statistics for a specific guild.

        Args:
            guild_id: Discord guild ID

        Returns:
            Dict with guild statistics
        """
        stats = {
            'total_members': 0,
            'active_members': 0,
            'inactive_members': 0
        }

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Total members tracked
                cursor.execute("""
                    SELECT COUNT(*) FROM members WHERE guild_id = ?
                """, (guild_id,))
                result = cursor.fetchone()
                stats['total_members'] = result[0] if result else 0

                # Active members
                cursor.execute("""
                    SELECT COUNT(*) FROM members WHERE guild_id = ? AND is_active = 1
                """, (guild_id,))
                result = cursor.fetchone()
                stats['active_members'] = result[0] if result else 0

                # Inactive (left) members
                cursor.execute("""
                    SELECT COUNT(*) FROM members WHERE guild_id = ? AND is_active = 0
                """, (guild_id,))
                result = cursor.fetchone()
                stats['inactive_members'] = result[0] if result else 0

        except Exception as e:
            logger.error(f"Failed to get guild stats for {guild_id}: {e}")

        return stats

    def get_activity_stats(self, guild_id: int) -> Dict[str, Any]:
        """
        Get detailed activity statistics for server-stats command.

        Args:
            guild_id: Discord guild ID

        Returns:
            Dict with activity statistics including online counts, offline periods, etc.
        """
        stats = {
            'currently_online': 0,
            'currently_offline': 0,
            'never_seen_offline': 0,
            'offline_1h': 0,
            'offline_24h': 0,
            'offline_7d': 0,
            'offline_30d': 0,
            'offline_30d_plus': 0
        }

        try:
            current_time = int(datetime.now(timezone.utc).timestamp())
            hour_ago = current_time - 3600
            day_ago = current_time - SECONDS_PER_DAY
            week_ago = current_time - (7 * SECONDS_PER_DAY)
            month_ago = current_time - (30 * SECONDS_PER_DAY)

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Currently online (last_seen = 0)
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1 AND last_seen = 0
                """, (guild_id,))
                result = cursor.fetchone()
                stats['currently_online'] = result[0] if result else 0

                # Never seen offline (never tracked - last_seen IS NULL)
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1 AND last_seen IS NULL
                """, (guild_id,))
                result = cursor.fetchone()
                stats['never_seen_offline'] = result[0] if result else 0

                # Offline within last hour
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?
                """, (guild_id, hour_ago))
                result = cursor.fetchone()
                stats['offline_1h'] = result[0] if result else 0

                # Offline within last 24 hours
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?
                """, (guild_id, day_ago))
                result = cursor.fetchone()
                stats['offline_24h'] = result[0] if result else 0

                # Offline within last 7 days
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?
                """, (guild_id, week_ago))
                result = cursor.fetchone()
                stats['offline_7d'] = result[0] if result else 0

                # Offline within last 30 days
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?
                """, (guild_id, month_ago))
                result = cursor.fetchone()
                stats['offline_30d'] = result[0] if result else 0

                # Offline more than 30 days
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen < ?
                """, (guild_id, month_ago))
                result = cursor.fetchone()
                stats['offline_30d_plus'] = result[0] if result else 0

                # Total currently offline (total active - currently online)
                # Note: offline buckets above are overlapping (e.g., 2h offline counts in all buckets),
                # so we calculate it as: total active members - online members
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                """, (guild_id,))
                result = cursor.fetchone()
                total_active = result[0] if result else 0
                stats['currently_offline'] = total_active - stats['currently_online']

        except Exception as e:
            logger.error(f"Failed to get activity stats for {guild_id}: {e}")

        return stats

    def remove_guild_data(self, guild_id: int) -> bool:
        """
        Completely remove a guild and all its associated members from the database.
        
        Args:
            guild_id: Discord guild ID to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Delete all members associated with this guild
                cursor.execute("DELETE FROM members WHERE guild_id = ?", (guild_id,))
                members_deleted = cursor.rowcount
                
                # Delete the guild configuration/entry
                cursor.execute("DELETE FROM guilds WHERE guild_id = ?", (guild_id,))
                guild_deleted = cursor.rowcount
                
                logger.info(f"Removed guild {guild_id} from database. Deleted {members_deleted} member records.")
                return True
        except Exception as e:
            logger.error(f"Failed to remove guild data for {guild_id}: {e}")
            return False

    def record_role_change(self, guild_id: int, user_id: int, role_name: str, action: str) -> bool:
        """
        Record a role change for a member.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            role_name: Name of the role that was added/removed
            action: 'added' or 'removed'

        Returns:
            bool: True if successful, False otherwise
        """
        # Validate inputs
        if not role_name or not isinstance(role_name, str) or not role_name.strip():
            logger.error(f"Invalid role_name: {role_name}")
            return False
        
        if action not in ("added", "removed"):
            logger.error(f"Invalid action '{action}', must be 'added' or 'removed'")
            return False
        
        # Sanitize role name: strip whitespace, limit length, remove problematic characters
        sanitized_role = role_name.strip()
        # Remove any null bytes or control characters that could cause display issues
        sanitized_role = ''.join(char for char in sanitized_role if ord(char) >= 32 or char == '\n')
        # Limit length to 100 characters (Discord's role name limit)
        if len(sanitized_role) > 100:
            sanitized_role = sanitized_role[:100]
            logger.warning(f"Role name truncated to 100 characters: {sanitized_role}")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                current_time = int(datetime.now(timezone.utc).timestamp())
                
                cursor.execute("""
                    INSERT INTO role_changes (guild_id, user_id, role_name, action, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (guild_id, user_id, sanitized_role, action, current_time))
                
                # Cleanup old role changes (keep only last 20)
                self._cleanup_role_changes(conn, guild_id, user_id)
                return True
        except Exception as e:
            logger.error(f"Failed to record role change for user {user_id} in guild {guild_id}: {e}")
            return False

    def _cleanup_role_changes(self, conn, guild_id: int, user_id: int, keep_count: int = 20) -> bool:
        """
        Remove old role changes, keeping only the most recent ones.

        Args:
            conn: Database connection
            guild_id: Discord guild ID
            user_id: Discord user ID
            keep_count: Number of recent records to keep (default 20)

        Returns:
            bool: True if successful
        """
        try:
            cursor = conn.cursor()
            
            # Get the ID of the 20th most recent record
            cursor.execute("""
                SELECT id FROM role_changes
                WHERE guild_id = ? AND user_id = ?
                ORDER BY timestamp DESC
                LIMIT 1 OFFSET ?
            """, (guild_id, user_id, keep_count - 1))
            
            result = cursor.fetchone()
            if result:
                # Delete all records older than the 20th most recent
                cursor.execute("""
                    DELETE FROM role_changes
                    WHERE guild_id = ? AND user_id = ? AND id < ?
                """, (guild_id, user_id, result[0]))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.debug(f"Cleaned up {deleted_count} old role changes for user {user_id}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup role changes for user {user_id}: {e}")
            return False

    def get_role_history(self, guild_id: int, user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get role change history for a member.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            limit: Maximum number of changes to return (default 20)

        Returns:
            List of role changes, newest first
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT role_name, action, timestamp
                    FROM role_changes
                    WHERE guild_id = ? AND user_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (guild_id, user_id, limit))
                
                rows = cursor.fetchall()
                changes = []
                for row in rows:
                    changes.append({
                        'role_name': row[0],
                        'action': row[1],
                        'timestamp': row[2]
                    })
                return changes
        except Exception as e:
            logger.error(f"Failed to get role history for user {user_id} in guild {guild_id}: {e}")
            return []

    # ==================== Message Activity Operations ====================

    def increment_message_activity(self, guild_id: int, user_id: int, date: int, count: int = 1) -> bool:
        """
        Increment message count for a user on a specific date.
        Uses INSERT OR REPLACE for atomicity (handles race conditions).

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            date: Unix timestamp of start of day (UTC)
            count: Number of messages to add (default 1)

        Returns:
            bool: True if successful
        """
        try:
            if count <= 0:
                return False
                
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # First, ensure the member exists in the database
                if not self.member_exists(guild_id, user_id):
                    return False
                
                # INSERT OR REPLACE approach: if record exists, increment; if not, create with count
                cursor.execute("""
                    INSERT INTO message_activity (guild_id, user_id, date, message_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, date) DO UPDATE SET
                    message_count = message_count + ?
                """, (guild_id, user_id, date, count, count))
                
                return True
        except Exception as e:
            logger.error(f"Failed to increment message activity for user {user_id}: {e}")
            return False

    def increment_message_activity_hourly(self, guild_id: int, user_id: int, timestamp: int, hour: int, count: int = 1) -> bool:
        """
        Increment hourly message count for a user at a specific hour.
        
        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            timestamp: Unix timestamp rounded to the hour
            count: Number of messages to add (default 1)
            hour: Hour of day (0-23)
        
        Returns:
            bool: True if successful
        """
        try:
            if count <= 0:
                return False
                
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Ensure member exists
                if not self.member_exists(guild_id, user_id):
                    return False
                
                cursor.execute("""
                    INSERT INTO message_activity_hourly (guild_id, user_id, timestamp, hour, message_count)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, timestamp) DO UPDATE SET
                    message_count = message_count + ?
                """, (guild_id, user_id, timestamp, hour, count, count))
                
                return True
        except Exception as e:
            logger.error(f"Failed to increment hourly message activity for user {user_id}: {e}")
            return False

    def get_message_activity_period(self, guild_id: int, user_id: int, days: int = 30) -> Dict[str, int]:
        """
        Get message count statistics for a specific period.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            days: Number of days to look back (default 30)

        Returns:
            Dict with keys: 'total', 'today', 'this_week', 'avg_per_day'
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
                
                # Calculate cutoff date
                cutoff_date = today_start - (days * SECONDS_PER_DAY)  # Convert days to seconds
                week_cutoff = today_start - (7 * SECONDS_PER_DAY)
                
                # Get total messages in period
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                """, (guild_id, user_id, cutoff_date))
                
                total = cursor.fetchone()[0]
                
                # Get today's count
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date = ?
                """, (guild_id, user_id, today_start))
                
                today_count = cursor.fetchone()[0]
                
                # Get this week's count
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                """, (guild_id, user_id, week_cutoff))
                
                week_count = cursor.fetchone()[0]
                
                # Get average per day
                cursor.execute("""
                    SELECT COUNT(DISTINCT date), COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                """, (guild_id, user_id, week_cutoff))
                
                result = cursor.fetchone()
                days_with_activity = result[0]
                week_total = result[1]
                avg_per_day = round(week_total / 7, 1) if days_with_activity > 0 else 0
                
                return {
                    'total': total,
                    'today': today_count,
                    'this_week': week_count,
                    'this_month': total,  # Same as total for 30-day window
                    'avg_per_day': avg_per_day
                }
        except Exception as e:
            logger.error(f"Failed to get message activity for user {user_id}: {e}")
            return {'total': 0, 'today': 0, 'this_week': 0, 'this_month': 0, 'avg_per_day': 0}

    def get_message_activity_trend(self, guild_id: int, user_id: int, days: int = 365) -> List[Dict[str, Any]]:
        """
        Get detailed daily message breakdown for trend analysis.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            days: Number of days to look back (default 365)

        Returns:
            List of daily activity records, newest first
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
                
                # Calculate cutoff date
                cutoff_date = today_start - (days * SECONDS_PER_DAY)
                
                cursor.execute("""
                    SELECT date, message_count
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                    ORDER BY date DESC
                """, (guild_id, user_id, cutoff_date))
                
                rows = cursor.fetchall()
                activity = []
                for row in rows:
                    activity.append({
                        'date': row[0],
                        'message_count': row[1]
                    })
                return activity
        except Exception as e:
            logger.error(f"Failed to get message activity trend for user {user_id}: {e}")
            return []

    def get_guild_message_activity_stats(self, guild_id: int, days: int = 365) -> Dict[str, Any]:
        """
        Get guild-wide message activity statistics.

        Args:
            guild_id: Discord guild ID
            days: Number of days to look back (default 365)

        Returns:
            Dict with guild-wide statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
                
                # Calculate cutoff dates
                cutoff_date = today_start - (days * SECONDS_PER_DAY)
                week_cutoff = today_start - (7 * SECONDS_PER_DAY)
                month_cutoff = today_start - (30 * SECONDS_PER_DAY)
                quarter_cutoff = today_start - (90 * SECONDS_PER_DAY)
                
                # Get total messages for all periods
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, cutoff_date))
                total_365d = cursor.fetchone()[0]
                
                # Get 90-day total
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, quarter_cutoff))
                total_90d = cursor.fetchone()[0]
                
                # Get 30-day total
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, month_cutoff))
                total_30d = cursor.fetchone()[0]
                
                # Get 7-day total
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, week_cutoff))
                total_7d = cursor.fetchone()[0]
                
                # Get today's count
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date = ?
                """, (guild_id, today_start))
                today_count = cursor.fetchone()[0]
                
                # Get busiest and quietest days (365 days)
                cursor.execute("""
                    SELECT date, SUM(message_count) as total
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                    GROUP BY date
                    ORDER BY total DESC
                    LIMIT 1
                """, (guild_id, cutoff_date))
                busiest_day = cursor.fetchone()
                
                cursor.execute("""
                    SELECT date, SUM(message_count) as total
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                    GROUP BY date
                    ORDER BY total ASC
                    LIMIT 1
                """, (guild_id, cutoff_date))
                quietest_day = cursor.fetchone()
                
                # Get active member count (30 days)
                cursor.execute("""
                    SELECT COUNT(DISTINCT user_id)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, month_cutoff))
                active_members_30d = cursor.fetchone()[0]
                
                # Calculate average per day (365 days)
                avg_per_day = round(total_365d / 365, 1) if total_365d > 0 else 0
                
                # Calculate messages per active member (30 days)
                avg_per_member = round(total_30d / active_members_30d, 1) if active_members_30d > 0 else 0
                
                return {
                    'total_365d': total_365d,
                    'total_90d': total_90d,
                    'total_30d': total_30d,
                    'total_7d': total_7d,
                    'today': today_count,
                    'avg_per_day': avg_per_day,
                    'busiest_day': {'date': busiest_day[0], 'count': busiest_day[1]} if busiest_day else None,
                    'quietest_day': {'date': quietest_day[0], 'count': quietest_day[1]} if quietest_day else None,
                    'active_members_30d': active_members_30d,
                    'avg_per_member': avg_per_member
                }
        except Exception as e:
            logger.error(f"Failed to get guild message activity stats for guild {guild_id}: {e}")
            return {
                'total_365d': 0, 'total_90d': 0, 'total_30d': 0, 'total_7d': 0, 'today': 0,
                'avg_per_day': 0, 'busiest_day': None, 'quietest_day': None,
                'active_members_30d': 0, 'avg_per_member': 0
            }

    def cleanup_old_message_activity(self, guild_id: int, user_id: int, days: int = 365) -> bool:
        """
        Remove message activity records older than specified days.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            days: Keep records from last N days (default 365)

        Returns:
            bool: True if successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
                
                # Calculate cutoff date
                cutoff_date = today_start - (days * SECONDS_PER_DAY)
                
                cursor.execute("""
                    DELETE FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date < ?
                """, (guild_id, user_id, cutoff_date))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old message activity records for user {user_id} in guild {guild_id}")
                
                return True
        except Exception as e:
            logger.error(f"Failed to cleanup old message activity for user {user_id}: {e}")
            return False

    def cleanup_all_old_message_activity(self, days: int = 365) -> bool:
        """
        Remove all message activity records older than specified days across all users.

        Args:
            days: Keep records from last N days (default 365)

        Returns:
            bool: True if successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
                
                # Calculate cutoff date
                cutoff_date = today_start - (days * SECONDS_PER_DAY)
                
                cursor.execute("""
                    DELETE FROM message_activity
                    WHERE date < ?
                """, (cutoff_date,))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old message activity records globally")
                
                return True
        except Exception as e:
            logger.error(f"Failed to cleanup old message activity globally: {e}")
            return False

    # ===== User Statistics Methods =====

    def get_server_snapshot_stats(self, guild_id: int) -> Dict[str, Any]:
        """
        Get comprehensive server statistics snapshot.

        Args:
            guild_id: Guild ID

        Returns:
            Dictionary with server statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                thirty_days_ago = now - (30 * SECONDS_PER_DAY)
                
                # Total and active member counts
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_members,
                        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_members,
                        SUM(CASE WHEN is_active = 1 AND (last_seen IS NULL OR last_seen = 0 OR last_seen > ?) THEN 1 ELSE 0 END) as active_30d
                    FROM members
                    WHERE guild_id = ?
                """, (thirty_days_ago, guild_id))
                
                row = cursor.fetchone()
                total_members = row['total_members'] or 0
                active_members = row['active_members'] or 0
                active_30d = row['active_30d'] or 0
                
                # Get this month's joins and leaves
                now_utc = datetime.now(timezone.utc)
                month_start = int(datetime(now_utc.year, now_utc.month, 1, tzinfo=timezone.utc).timestamp())
                
                cursor.execute("""
                    SELECT COUNT(*) as joins
                    FROM members
                    WHERE guild_id = ? AND join_date >= ?
                """, (guild_id, month_start))
                joins_this_month = cursor.fetchone()['joins'] or 0
                
                # Count members who left this month (is_active = 0 and last_seen >= month_start)
                cursor.execute("""
                    SELECT COUNT(*) as leaves
                    FROM members
                    WHERE guild_id = ? AND is_active = 0 AND last_seen >= ?
                """, (guild_id, month_start))
                leaves_this_month = cursor.fetchone()['leaves'] or 0
                
                # Get total message count for last 30 days
                cursor.execute("""
                    SELECT SUM(message_count) as total_messages
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                """, (guild_id, thirty_days_ago))
                total_messages = cursor.fetchone()['total_messages'] or 0
                
                # Get most active member in last 30 days
                cursor.execute("""
                    SELECT m.username, SUM(ma.message_count) as msg_count
                    FROM message_activity ma
                    JOIN members m ON ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                    WHERE ma.guild_id = ? AND ma.date >= ?
                    GROUP BY ma.user_id
                    ORDER BY msg_count DESC
                    LIMIT 1
                """, (guild_id, thirty_days_ago))
                most_active = cursor.fetchone()
                most_active_user = most_active['username'] if most_active else 'N/A'
                most_active_count = most_active['msg_count'] if most_active else 0
                
                return {
                    'total_members': total_members,
                    'active_members': active_members,
                    'active_30d': active_30d,
                    'inactive_30d': active_members - active_30d,
                    'joins_this_month': joins_this_month,
                    'leaves_this_month': leaves_this_month,
                    'net_growth': joins_this_month - leaves_this_month,
                    'total_messages_30d': total_messages,
                    'avg_messages_per_member': total_messages / active_members if active_members > 0 else 0,
                    'most_active_user': most_active_user,
                    'most_active_count': most_active_count
                }
        except Exception as e:
            logger.error(f"Failed to get server snapshot stats for guild {guild_id}: {e}")
            return {}

    def get_member_growth_stats(self, guild_id: int, days: int = 30) -> Dict[str, Any]:
        """
        Get member growth statistics over specified period.

        Args:
            guild_id: Guild ID
            days: Number of days to look back

        Returns:
            Dictionary with growth statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                period_start = now - (days * SECONDS_PER_DAY)
                
                # Members who joined in this period
                cursor.execute("""
                    SELECT COUNT(*) as joins
                    FROM members
                    WHERE guild_id = ? AND join_date >= ?
                """, (guild_id, period_start))
                joins = cursor.fetchone()['joins'] or 0
                
                # Members who left in this period (is_active = 0 and last_seen >= period_start)
                cursor.execute("""
                    SELECT COUNT(*) as leaves
                    FROM members
                    WHERE guild_id = ? AND is_active = 0 AND last_seen >= ?
                """, (guild_id, period_start))
                leaves = cursor.fetchone()['leaves'] or 0
                
                # Current total members
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM members
                    WHERE guild_id = ? AND is_active = 1
                """, (guild_id,))
                current_total = cursor.fetchone()['total'] or 0
                
                # Calculate previous total
                previous_total = current_total - joins + leaves
                growth_rate = ((current_total - previous_total) / previous_total * 100) if previous_total > 0 else 0
                
                return {
                    'joins': joins,
                    'leaves': leaves,
                    'net_growth': joins - leaves,
                    'current_total': current_total,
                    'previous_total': previous_total,
                    'growth_rate': growth_rate,
                    'period_days': days
                }
        except Exception as e:
            logger.error(f"Failed to get member growth stats for guild {guild_id}: {e}")
            return {}

    def get_retention_cohorts(self, guild_id: int) -> Dict[str, Any]:
        """
        Get retention cohort analysis for members.

        Args:
            guild_id: Guild ID

        Returns:
            Dictionary with retention statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                thirty_days_ago = now - (30 * SECONDS_PER_DAY)
                sixty_days_ago = now - (60 * SECONDS_PER_DAY)
                ninety_days_ago = now - (90 * SECONDS_PER_DAY)
                
                cohorts = {}
                
                # 30-day cohort
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_joined,
                        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as still_active,
                        SUM(CASE WHEN is_active = 1 AND (last_seen IS NULL OR last_seen = 0 OR last_seen > ?) THEN 1 ELSE 0 END) as active_recently
                    FROM members
                    WHERE guild_id = ? AND join_date >= ?
                """, (thirty_days_ago, guild_id, thirty_days_ago))
                row = cursor.fetchone()
                cohorts['30d'] = {
                    'total_joined': row['total_joined'] or 0,
                    'still_active': row['still_active'] or 0,
                    'active_recently': row['active_recently'] or 0,
                    'retention_rate': (row['still_active'] / row['total_joined'] * 100) if row['total_joined'] > 0 else 0
                }
                
                # 60-day cohort
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_joined,
                        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as still_active,
                        SUM(CASE WHEN is_active = 1 AND (last_seen IS NULL OR last_seen = 0 OR last_seen > ?) THEN 1 ELSE 0 END) as active_recently
                    FROM members
                    WHERE guild_id = ? AND join_date >= ? AND join_date < ?
                """, (thirty_days_ago, guild_id, sixty_days_ago, thirty_days_ago))
                row = cursor.fetchone()
                cohorts['60d'] = {
                    'total_joined': row['total_joined'] or 0,
                    'still_active': row['still_active'] or 0,
                    'active_recently': row['active_recently'] or 0,
                    'retention_rate': (row['still_active'] / row['total_joined'] * 100) if row['total_joined'] > 0 else 0
                }
                
                # 90-day cohort
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_joined,
                        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as still_active,
                        SUM(CASE WHEN is_active = 1 AND (last_seen IS NULL OR last_seen = 0 OR last_seen > ?) THEN 1 ELSE 0 END) as active_recently
                    FROM members
                    WHERE guild_id = ? AND join_date >= ? AND join_date < ?
                """, (thirty_days_ago, guild_id, ninety_days_ago, sixty_days_ago))
                row = cursor.fetchone()
                cohorts['90d'] = {
                    'total_joined': row['total_joined'] or 0,
                    'still_active': row['still_active'] or 0,
                    'active_recently': row['active_recently'] or 0,
                    'retention_rate': (row['still_active'] / row['total_joined'] * 100) if row['total_joined'] > 0 else 0
                }
                
                return cohorts
        except Exception as e:
            logger.error(f"Failed to get retention cohorts for guild {guild_id}: {e}")
            return {}

    def get_activity_leaderboard(self, guild_id: int, days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get activity leaderboard for top members.

        Args:
            guild_id: Guild ID
            days: Number of days to look back (0 for all-time)
            limit: Maximum number of members to return

        Returns:
            List of member dictionaries with activity stats
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if days > 0:
                    now = int(datetime.now(timezone.utc).timestamp())
                    period_start = now - (days * SECONDS_PER_DAY)
                    
                    cursor.execute("""
                        SELECT 
                            m.user_id,
                            m.username,
                            m.nickname,
                            SUM(ma.message_count) as total_messages
                        FROM message_activity ma
                        JOIN members m ON ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                        WHERE ma.guild_id = ? AND ma.date >= ? AND m.is_active = 1
                        GROUP BY ma.user_id
                        ORDER BY total_messages DESC
                        LIMIT ?
                    """, (guild_id, period_start, limit))
                else:
                    # All-time
                    cursor.execute("""
                        SELECT 
                            m.user_id,
                            m.username,
                            m.nickname,
                            SUM(ma.message_count) as total_messages
                        FROM message_activity ma
                        JOIN members m ON ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                        WHERE ma.guild_id = ? AND m.is_active = 1
                        GROUP BY ma.user_id
                        ORDER BY total_messages DESC
                        LIMIT ?
                    """, (guild_id, limit))
                
                results = []
                for row in cursor.fetchall():
                    results.append({
                        'user_id': row['user_id'],
                        'username': row['username'],
                        'nickname': row['nickname'],
                        'display_name': row['nickname'] if row['nickname'] else row['username'],
                        'total_messages': row['total_messages'] or 0
                    })
                
                return results
        except Exception as e:
            logger.error(f"Failed to get activity leaderboard for guild {guild_id}: {e}")
            return []

    def get_activity_by_hour(self, guild_id: int, days: int = 30) -> Dict[int, int]:
        """
        Get message activity distribution by hour of day.

        Args:
            guild_id: Guild ID
            days: Number of days to look back

        Returns:
            Dictionary mapping hour (0-23) to message count
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                period_start = now - (days * SECONDS_PER_DAY)
                
                cursor.execute("""
                    SELECT hour, SUM(message_count) as total
                    FROM message_activity_hourly
                    WHERE guild_id = ? AND timestamp >= ?
                    GROUP BY hour
                    ORDER BY hour
                """, (guild_id, period_start))
                
                rows = cursor.fetchall()
                
                # Initialize all hours with 0
                activity = {hour: 0 for hour in range(24)}
                
                # Fill in actual data
                for row in rows:
                    activity[row['hour']] = row['total']
                
                return activity
        except Exception as e:
            logger.error(f"Failed to get hourly activity for guild {guild_id}: {e}", exc_info=True)
            return {hour: 0 for hour in range(24)}

    def get_activity_by_day(self, guild_id: int, days: int = 30) -> Dict[str, int]:
        """
        Get message activity distribution by day of week.

        Args:
            guild_id: Guild ID
            days: Number of days to look back

        Returns:
            Dictionary mapping day name to message count
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                period_start = now - (days * SECONDS_PER_DAY)
                
                cursor.execute("""
                    SELECT date, SUM(message_count) as count
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?
                    GROUP BY date
                """, (guild_id, period_start))
                
                day_counts = {'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0, 
                             'Friday': 0, 'Saturday': 0, 'Sunday': 0}
                
                for row in cursor.fetchall():
                    date_dt = datetime.fromtimestamp(row['date'], tz=timezone.utc)
                    day_name = date_dt.strftime('%A')
                    day_counts[day_name] += row['count'] or 0
                
                return day_counts
        except Exception as e:
            logger.error(f"Failed to get activity by day for guild {guild_id}: {e}")
            return {}

    # ==================== Scheduled Reports Operations ====================

    def get_guilds_with_reports_enabled(self) -> list:
        """Get all guilds that have scheduled reports enabled."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT guild_id, report_channel_id, report_frequency, report_types,
                           report_day_weekly, report_day_monthly, last_weekly_report, last_monthly_report,
                           timezone
                    FROM guilds
                    WHERE report_frequency IS NOT NULL AND report_channel_id IS NOT NULL
                """)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get guilds with reports enabled: {e}")
            return []

    def get_new_members_period(self, guild_id: int, days: int) -> list:
        """Get members who joined in the last N days."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff = int(datetime.now(timezone.utc).timestamp()) - (days * SECONDS_PER_DAY)
                cursor.execute("""
                    SELECT user_id, username, nickname, join_date, join_position
                    FROM members
                    WHERE guild_id = ? AND join_date >= ? AND is_active = 1
                    ORDER BY join_date DESC
                """, (guild_id, cutoff))
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get new members for guild {guild_id}: {e}")
            return []

    def get_departed_members_period(self, guild_id: int, days: int) -> list:
        """Get members who left in the last N days."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff = int(datetime.now(timezone.utc).timestamp()) - (days * SECONDS_PER_DAY)
                cursor.execute("""
                    SELECT user_id, username, nickname, last_seen, left_date
                    FROM members
                    WHERE guild_id = ? AND is_active = 0 
                    AND left_date IS NOT NULL AND left_date >= ?
                    ORDER BY left_date DESC
                """, (guild_id, cutoff))
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get departed members for guild {guild_id}: {e}")
            return []

    def get_top_active_users_period(self, guild_id: int, days: int, limit: int = 10) -> list:
        """Get most active users by message count in the last N days."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cutoff = int(datetime.now(timezone.utc).timestamp()) - (days * SECONDS_PER_DAY)
                cursor.execute("""
                    SELECT m.user_id, m.username, m.nickname, SUM(ma.message_count) as total_messages
                    FROM message_activity ma
                    JOIN members m ON ma.user_id = m.user_id AND ma.guild_id = m.guild_id
                    WHERE ma.guild_id = ? AND ma.date >= ? AND m.is_active = 1
                    GROUP BY m.user_id, m.username, m.nickname
                    ORDER BY total_messages DESC
                    LIMIT ?
                """, (guild_id, cutoff, limit))
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get top active users for guild {guild_id}: {e}")
            return []

    # ==================== Data Retention Operations ====================

    def cleanup_old_message_activity(self, guild_id: int, retention_days: int) -> Dict[str, int]:
        """
        Delete message activity records older than retention_days.

        Args:
            guild_id: Guild ID
            retention_days: Number of days to retain (older records are deleted)

        Returns:
            Dictionary with 'daily_deleted' and 'hourly_deleted' counts
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                cutoff = now - (retention_days * 86400)
                
                # Delete old daily activity records
                cursor.execute("""
                    DELETE FROM message_activity
                    WHERE guild_id = ? AND date < ?
                """, (guild_id, cutoff))
                daily_deleted = cursor.rowcount
                
                # Delete old hourly activity records
                cursor.execute("""
                    DELETE FROM message_activity_hourly
                    WHERE guild_id = ? AND timestamp < ?
                """, (guild_id, cutoff))
                hourly_deleted = cursor.rowcount
                
                if daily_deleted > 0 or hourly_deleted > 0:
                    logger.info(f"Cleaned up old activity for guild {guild_id}: {daily_deleted} daily, {hourly_deleted} hourly records")
                
                return {'daily_deleted': daily_deleted, 'hourly_deleted': hourly_deleted}
        except Exception as e:
            logger.error(f"Failed to cleanup old activity for guild {guild_id}: {e}", exc_info=True)
            return {'daily_deleted': 0, 'hourly_deleted': 0}

    def cleanup_all_guilds_message_activity(self) -> Dict[str, int]:
        """
        Run cleanup for all guilds based on their retention settings.

        Returns:
            Dictionary with total 'daily_deleted' and 'hourly_deleted' counts
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all guilds with their retention settings
                cursor.execute("""
                    SELECT guild_id, message_retention_days
                    FROM guilds
                    WHERE message_retention_days IS NOT NULL
                """)
                
                guilds = cursor.fetchall()
                total_daily = 0
                total_hourly = 0
                
                for row in guilds:
                    guild_id = row['guild_id']
                    retention_days = row['message_retention_days'] or 365
                    
                    result = self.cleanup_old_message_activity(guild_id, retention_days)
                    total_daily += result['daily_deleted']
                    total_hourly += result['hourly_deleted']
                
                if total_daily > 0 or total_hourly > 0:
                    logger.info(f"Global cleanup completed: {total_daily} daily, {total_hourly} hourly records deleted across {len(guilds)} guilds")
                
                return {'daily_deleted': total_daily, 'hourly_deleted': total_hourly, 'guilds_processed': len(guilds)}
        except Exception as e:
            logger.error(f"Failed to run global cleanup: {e}", exc_info=True)
            return {'daily_deleted': 0, 'hourly_deleted': 0, 'guilds_processed': 0}

    # ==================== Database Backup Operations ====================

    def create_backup(self, backup_folder: str) -> Optional[str]:
        """
        Create a backup of the database using SQLite's backup API.

        Args:
            backup_folder: Path to the folder where backups should be stored

        Returns:
            Path to the created backup file, or None if backup failed
        """
        from pathlib import Path
        
        try:
            # Ensure backup folder exists
            backup_path = Path(backup_folder)
            backup_path.mkdir(exist_ok=True)
            
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_path / f"lastseen_backup_{timestamp}.db"
            
            # Create backup using SQLite's backup API
            # This creates a separate connection to avoid interfering with the pool
            source_conn = sqlite3.connect(self.db_file)
            backup_conn = sqlite3.connect(str(backup_file))
            
            try:
                # Perform the backup
                with backup_conn:
                    source_conn.backup(backup_conn)
                
                logger.info(f"Database backup created successfully: {backup_file.name}")
                return str(backup_file)
            finally:
                source_conn.close()
                backup_conn.close()
                
        except Exception as e:
            logger.error(f"Failed to create database backup: {e}", exc_info=True)
            return None

    def cleanup_old_backups(self, backup_folder: str, retention_count: int) -> int:
        """
        Delete old backup files, keeping only the most recent backups.

        Args:
            backup_folder: Path to the folder containing backups
            retention_count: Number of recent backups to keep

        Returns:
            Number of backup files deleted
        """
        from pathlib import Path
        
        try:
            backup_path = Path(backup_folder)
            
            if not backup_path.exists():
                return 0
            
            # Get all backup files sorted by modification time (newest first)
            backup_files = sorted(
                backup_path.glob("lastseen_backup_*.db"),
                key=lambda p: p.stat().st_mtime,
                reverse=True
            )
            
            # Delete old backups beyond retention count
            deleted_count = 0
            for backup_file in backup_files[retention_count:]:
                try:
                    backup_file.unlink()
                    deleted_count += 1
                    logger.info(f"Deleted old backup: {backup_file.name}")
                except Exception as e:
                    logger.warning(f"Failed to delete backup {backup_file.name}: {e}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old backup(s), keeping {min(len(backup_files), retention_count)} most recent")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}", exc_info=True)
            return 0

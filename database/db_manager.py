"""Database manager for LastSeen bot with proper connection handling."""

import sqlite3
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database connections and operations."""

    def __init__(self, db_file: str):
        """
        Initialize database manager.

        Args:
            db_file: Path to SQLite database file
        """
        self.db_file = db_file
        self._initialize_database()

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures connections are properly closed.

        Yields:
            sqlite3.Connection: Database connection
        """
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row  # Access columns by name
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()

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
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO members
                    (guild_id, user_id, username, nickname, join_date, last_seen, is_active, roles)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                """, (guild_id, user_id, username, nickname, join_date, 0, roles_json))
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

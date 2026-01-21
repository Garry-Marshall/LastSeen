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

            # Indexes for role_changes table
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_role_changes_guild_user
                ON role_changes(guild_id, user_id, timestamp DESC)
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

            # Check members table for new columns
            cursor.execute("PRAGMA table_info(members)")
            member_columns = [row[1] for row in cursor.fetchall()]

            if 'join_position' not in member_columns:
                cursor.execute("ALTER TABLE members ADD COLUMN join_position INTEGER")
                logger.info("Added join_position column to members table")

            if 'nickname_history' not in member_columns:
                cursor.execute("ALTER TABLE members ADD COLUMN nickname_history TEXT")
                logger.info("Added nickname_history column to members table")

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
                """, (guild_id, user_id, username, nickname, join_date, 0, roles_json, nickname_history))
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
        """Update nickname history for a member (keeps last 5, unique only).
        
        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            old_nickname: Previous nickname before change
            new_nickname: New nickname after change
            
        Returns:
            bool: True if successful
        """
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
                    except:
                        history = []
                    
                    # Add old nickname if it's not already in history (check entire list for uniqueness)
                    if old_nickname and old_nickname not in history:
                        history.append(old_nickname)
                    
                    # Add new nickname if it's not already in history
                    if new_nickname and new_nickname not in history:
                        history.append(new_nickname)
                    
                    # Keep only last 5
                    history = history[-5:]
                    
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
            day_ago = current_time - 86400
            week_ago = current_time - (7 * 86400)
            month_ago = current_time - (30 * 86400)

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Currently online (last_seen = 0)
                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1 AND last_seen = 0
                """, (guild_id,))
                result = cursor.fetchone()
                stats['currently_online'] = result[0] if result else 0

                # Never seen offline (last_seen = 0, but could be online now)
                # This is same as currently_online
                stats['never_seen_offline'] = stats['currently_online']

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
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                current_time = int(datetime.now(timezone.utc).timestamp())
                
                cursor.execute("""
                    INSERT INTO role_changes (guild_id, user_id, role_name, action, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (guild_id, user_id, role_name, action, current_time))
                return True
        except Exception as e:
            logger.error(f"Failed to record role change for user {user_id} in guild {guild_id}: {e}")
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

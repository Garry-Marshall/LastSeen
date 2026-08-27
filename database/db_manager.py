"""Database manager for LastSeen bot with proper connection handling."""

import os
import sqlite3
import json
import logging
import threading
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Queue, Empty, Full

logger = logging.getLogger(__name__)

# Time constants (in seconds)
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600

# How many days of health_snapshots to keep. The Community Pulse trend chart
# only ever reads the newest ~84 days (get_health_history samples 12 points at
# 7-day steps), so 120 keeps the chart fully intact with margin while capping
# the table's otherwise-unbounded daily growth.
HEALTH_SNAPSHOT_RETENTION_DAYS = 120

# How long global /about statistics stay cached (seconds)
BOT_STATS_CACHE_TTL = 300


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

        # Cached global counts for /about (see get_bot_statistics)
        self._bot_stats_cache: Optional[dict] = None
        self._bot_stats_expires = 0
        self._bot_stats_lock = threading.Lock()
        
        # Initialize the connection pool
        self._initialize_pool()
        self._initialize_database()
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create and configure a new database connection."""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Enable foreign key enforcement so ON DELETE CASCADE works on all tables
        conn.execute("PRAGMA foreign_keys = ON")
        # WAL lets readers and a writer proceed concurrently (important with a
        # threaded connection pool); busy_timeout avoids immediate "database is
        # locked" errors under contention.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        # Cap the -wal file: any successful checkpoint (including SQLite's
        # automatic passive ones) truncates it back to this size. Without it,
        # only an uncontended TRUNCATE checkpoint ever shrinks the file, and
        # with constant concurrent worker-thread readers those rarely win.
        conn.execute("PRAGMA journal_size_limit = 67108864")  # 64 MB
        return conn

    def _initialize_pool(self):
        """Initialize the connection pool with connections."""
        for _ in range(self.pool_size):
            conn = self._create_connection()
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
            return self._create_connection()
    
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

    def _wal_size_mb(self) -> float:
        """Current size of the -wal file in MB (0.0 if absent/unreadable)."""
        try:
            return os.path.getsize(f"{self.db_file}-wal") / (1024 * 1024)
        except OSError:
            return 0.0

    def checkpoint_wal(self, mode: str = "TRUNCATE") -> bool:
        """Checkpoint the WAL into the main database.

        PASSIVE copies whatever frames it can without waiting on readers or
        writers — it never blocks, and with journal_size_limit set the file
        shrinks on success. TRUNCATE additionally waits (up to busy_timeout)
        for all readers to clear so it can reset the file completely; under
        constant concurrent worker-thread readers it frequently loses that
        race, which is why it is only attempted periodically and its failure
        is logged loudly with the frame counts and file size.

        Args:
            mode: 'PASSIVE', 'FULL', 'RESTART', or 'TRUNCATE'

        Returns:
            bool: True if the checkpoint completed without being blocked.
        """
        if mode not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
            raise ValueError(f"Invalid checkpoint mode: {mode}")

        conn = self._get_connection_from_pool()
        try:
            row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
            # row = (busy, log_frames, checkpointed_frames); busy=0 means it ran
            busy, log_frames, checkpointed = (row[0], row[1], row[2]) if row else (1, -1, -1)
            if busy:
                # Expected occasionally for blocking modes; a PASSIVE busy=1
                # (another checkpoint already running) is unremarkable.
                log = logger.warning if mode != "PASSIVE" else logger.debug
                log(
                    f"WAL {mode} checkpoint blocked "
                    f"(checkpointed {checkpointed}/{log_frames} frames, wal file {self._wal_size_mb():.1f} MB); "
                    f"will retry next cycle"
                )
                return False
            logger.debug(f"WAL {mode} checkpoint ok ({checkpointed}/{log_frames} frames, wal file {self._wal_size_mb():.1f} MB)")
            return True
        except Exception as e:
            logger.error(f"WAL checkpoint failed: {e}")
            return False
        finally:
            self._return_connection_to_pool(conn)

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

    @contextmanager
    def _borrow(self, conn: Optional[sqlite3.Connection]):
        """Yield a connection, reusing `conn` if given or borrowing one otherwise.

        Lets a read-only query method either open its own pooled connection (the
        default) or run on a caller-supplied connection so several such methods
        can share a single round-trip. A borrowed connection is returned to the
        pool here; a supplied one is left for the caller to manage. Read-only, so
        no commit is issued in either case.
        """
        if conn is not None:
            yield conn
        else:
            own = self._get_connection_from_pool()
            try:
                yield own
            finally:
                self._return_connection_to_pool(own)

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

            if 'language' not in columns:
                cursor.execute("ALTER TABLE guilds ADD COLUMN language TEXT DEFAULT 'en'")
                logger.info("Added language column to guilds table")

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

            # Index for departed-member queries (created after the column exists)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_members_left_date
                ON members(guild_id, left_date)
                WHERE left_date IS NOT NULL
            """)

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

            # Privacy opt-out list. Global (per-user, not per-guild) and has no
            # FK to guilds so it survives guild removal.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS opted_out_users (
                    user_id INTEGER PRIMARY KEY,
                    opted_out_at INTEGER NOT NULL
                )
            """)

            # Watchlists: admin-configured presence alerts. Stores only alert
            # config plus minimal fire-state (never presence history). All
            # timing decisions read the existing single members.last_seen column.
            #   target_type: 'user' (target_id = user_id) | 'role' (target_id = role_id)
            #   alert_type : 'online_return' (edge-triggered) | 'offline_for' (swept)
            #   threshold_seconds: online_return = optional minimum-away gate;
            #                      offline_for = required offline duration
            #   channel_id : where alerts post (the channel the watch was created in)
            #   state      : offline_for user watches — 'armed' | 'triggered'
            #   fired_targets: JSON user_id list already alerted (role offline_for dedupe)
            #   last_fired_at: online_return cooldown bookkeeping
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq INTEGER,
                    guild_id INTEGER NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id INTEGER NOT NULL,
                    alert_type TEXT NOT NULL,
                    threshold_seconds INTEGER,
                    channel_id INTEGER,
                    state TEXT DEFAULT 'armed',
                    fired_targets TEXT,
                    created_by INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(guild_id, target_type, target_id, alert_type),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_watchlists_guild
                ON watchlists(guild_id)
            """)

            # Returning members: one row per time a member came back online after
            # being offline for at least the return threshold (30 days). This is
            # the only place a past absence length is persisted — members.last_seen
            # is single-column and overwritten to 0 the moment they return, so the
            # away duration is captured here at the transition or lost forever.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS member_returns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    away_seconds INTEGER NOT NULL,
                    returned_at INTEGER NOT NULL,
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE,
                    FOREIGN KEY (guild_id, user_id) REFERENCES members(guild_id, user_id) ON DELETE CASCADE
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_member_returns_guild_time
                ON member_returns(guild_id, returned_at DESC)
            """)

            # Health snapshots: one guild-level aggregate row per UTC day,
            # written by the nightly snapshot task. Deliberately holds no
            # per-user data — only counts — so it can outlive message
            # retention pruning without touching data-minimization promises.
            # `date` is the UTC midnight the rolling 7-day window ends at.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS health_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    date INTEGER NOT NULL,
                    active_members INTEGER NOT NULL,
                    posters_7d INTEGER NOT NULL,
                    messages_7d INTEGER NOT NULL,
                    joins_7d INTEGER NOT NULL,
                    leaves_7d INTEGER NOT NULL,
                    returns_7d INTEGER NOT NULL,
                    UNIQUE(guild_id, date),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
                )
            """)

            # Member activity summary: one durable rollup row per member that
            # powers the Member Journey view. Every field folds forward as an
            # O(1) update at the member's first message of a new day (see
            # increment_message_activity), so the raw per-day message_activity
            # rows can keep being pruned on the normal retention schedule
            # without losing lifetime streaks/gaps. Like health_snapshots and
            # member_returns, this holds only derived counts — no presence, no
            # per-day history — so it costs nothing against data-minimization.
            #   first_active/last_active: UTC day-start of first/most-recent
            #     active day (matches message_activity.date granularity)
            #   current_streak: running consecutive-active-day count (bookkeeping
            #     needed to extend longest_streak); longest_gap is in whole days
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS member_activity_summary (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    first_active INTEGER,
                    last_active INTEGER,
                    total_messages INTEGER NOT NULL DEFAULT 0,
                    active_days INTEGER NOT NULL DEFAULT 0,
                    current_streak INTEGER NOT NULL DEFAULT 0,
                    longest_streak INTEGER NOT NULL DEFAULT 0,
                    longest_gap INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id),
                    FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE,
                    FOREIGN KEY (guild_id, user_id) REFERENCES members(guild_id, user_id) ON DELETE CASCADE
                )
            """)

            # One-time backfill: seed the summary from whatever message_activity
            # rows still exist (bounded by each guild's retention). Members whose
            # true first message predates the retained window can't be
            # reconstructed — their journey reads "since tracking began", exact
            # from here forward. Guarded on an empty table so it runs once.
            #
            # Join through members and guilds: production can hold orphaned
            # message_activity rows (member/guild gone while FK enforcement was
            # off at some past point), and INSERT OR IGNORE does NOT suppress
            # foreign-key violations — an orphan would abort the whole migration.
            # A member with no activity simply won't get a summary row until they
            # next post, which is fine.
            # One-time sweep of legacy orphaned activity: rows whose member no
            # longer exists. These date from a ~1-month window in early 2026
            # (message_activity was introduced 2026-01-21 with cascade FKs, but
            # PRAGMA foreign_keys=ON wasn't set until 2026-02-23), during which
            # member/guild deletions didn't cascade. Enforcement has been on
            # since, so no new orphans are produced — this is a one-time sweep,
            # a no-op on clean databases. It is tracked by PRAGMA user_version
            # (NOT the summary-backfill guard below): the two are independent
            # migrations, and the backfill may already have run on an earlier
            # startup, which would leave a summary-guarded cleanup stranded.
            # (member.guild_id references guilds, so a surviving member implies a
            # surviving guild; missing member is the meaningful orphan here.)
            cursor.execute("PRAGMA user_version")
            if cursor.fetchone()[0] < 1:
                orphan_daily = cursor.execute("""
                    DELETE FROM message_activity
                    WHERE NOT EXISTS (
                        SELECT 1 FROM members m
                        WHERE m.guild_id = message_activity.guild_id
                          AND m.user_id = message_activity.user_id
                    )
                """).rowcount
                orphan_hourly = cursor.execute("""
                    DELETE FROM message_activity_hourly
                    WHERE NOT EXISTS (
                        SELECT 1 FROM members m
                        WHERE m.guild_id = message_activity_hourly.guild_id
                          AND m.user_id = message_activity_hourly.user_id
                    )
                """).rowcount
                if orphan_daily or orphan_hourly:
                    logger.info(
                        f"Cleaned up orphaned activity rows: {orphan_daily} daily, "
                        f"{orphan_hourly} hourly"
                    )
                cursor.execute("PRAGMA user_version = 1")

            cursor.execute("SELECT COUNT(*) FROM member_activity_summary")
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    SELECT ma.guild_id, ma.user_id, ma.date, ma.message_count
                    FROM message_activity ma
                    JOIN members m ON m.guild_id = ma.guild_id AND m.user_id = ma.user_id
                    JOIN guilds g ON g.guild_id = ma.guild_id
                    ORDER BY ma.guild_id, ma.user_id, ma.date
                """)
                summaries: Dict[tuple, Dict[str, int]] = {}
                for gid, uid, date, mc in cursor.fetchall():
                    s = summaries.get((gid, uid))
                    if s is None:
                        summaries[(gid, uid)] = {
                            'first': date, 'last': date, 'total': mc,
                            'days': 1, 'cur': 1, 'best': 1, 'gap': 0,
                        }
                        continue
                    s['total'] += mc
                    gap = (date - s['last']) // SECONDS_PER_DAY - 1
                    if gap <= 0:
                        s['cur'] += 1
                    else:
                        if gap > s['gap']:
                            s['gap'] = gap
                        s['cur'] = 1
                    if s['cur'] > s['best']:
                        s['best'] = s['cur']
                    s['days'] += 1
                    s['last'] = date
                for (gid, uid), s in summaries.items():
                    cursor.execute("""
                        INSERT OR IGNORE INTO member_activity_summary
                            (guild_id, user_id, first_active, last_active,
                             total_messages, active_days, current_streak,
                             longest_streak, longest_gap)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (gid, uid, s['first'], s['last'], s['total'],
                          s['days'], s['cur'], s['best'], s['gap']))
                if summaries:
                    logger.info(f"Backfilled member_activity_summary for {len(summaries)} member(s)")

            # Migration: per-guild display number (`seq`). Databases created
            # before this column show the raw global autoincrement id (which
            # never resets or reuses removed numbers). Add the column and
            # backfill each guild's watches as compact 1..N by creation order.
            cursor.execute("PRAGMA table_info(watchlists)")
            watch_cols = [row[1] for row in cursor.fetchall()]
            if 'seq' not in watch_cols:
                cursor.execute("ALTER TABLE watchlists ADD COLUMN seq INTEGER")
                cursor.execute("SELECT id, guild_id FROM watchlists ORDER BY guild_id, id")
                counters: Dict[int, int] = {}
                for wid, gid in cursor.fetchall():
                    counters[gid] = counters.get(gid, 0) + 1
                    cursor.execute("UPDATE watchlists SET seq = ? WHERE id = ?", (counters[gid], wid))
                logger.info("Backfilled per-guild seq numbers for existing watches")

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
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at, bot_admin_role_name, user_role_name)
                    VALUES (?, ?, ?, ?, 'LastSeen Admin', 'LastSeen User')
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

    def set_guild_language(self, guild_id: int, language: str, guild_name: str = 'Unknown') -> bool:
        """Set the language for a guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # First ensure guild exists (in case it wasn't added via on_guild_join)
                cursor.execute("""
                    INSERT OR IGNORE INTO guilds (guild_id, guild_name, inactive_days, added_at)
                    VALUES (?, ?, 10, ?)
                """, (guild_id, guild_name, int(datetime.now(timezone.utc).timestamp())))

                # Now update the language and guild name if needed
                cursor.execute("""
                    UPDATE guilds
                    SET language = ?,
                        guild_name = CASE WHEN guild_name = 'Unknown' THEN ? ELSE guild_name END
                    WHERE guild_id = ?
                """, (language, guild_name, guild_id))
                return True
        except Exception as e:
            logger.error(f"Failed to set language for guild {guild_id}: {e}")
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

    _MAX_ROLE_NAME_LENGTH = 100

    def set_track_only_roles(self, guild_id: int, role_names: List[str], guild_name: str = 'Unknown') -> bool:
        """Set which roles should be tracked (empty list = track all)."""
        try:
            truncated = [r[:self._MAX_ROLE_NAME_LENGTH] for r in role_names if isinstance(r, str)]
            roles_json = json.dumps(truncated) if truncated else None
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

    def get_all_guild_ids(self) -> List[int]:
        """Get a list of all guild IDs currently in the database.

        Returns:
            List of guild IDs
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT guild_id FROM guilds")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get all guild IDs: {e}")
            return []

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
                # Upsert rather than INSERT OR REPLACE: REPLACE deletes the
                # existing member row first, and with FK enforcement on that
                # cascade-deletes role_changes, message_activity and
                # message_activity_hourly. ON CONFLICT DO UPDATE mutates the row
                # in place, so no cascade fires and historical columns
                # (last_seen, is_active, join_date, nickname_history,
                # join_position, left_date) are preserved. Only the current
                # profile fields are refreshed on conflict.
                cursor.execute("""
                    INSERT INTO members
                    (guild_id, user_id, username, nickname, join_date, last_seen, is_active, roles, nickname_history)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(guild_id, user_id) DO UPDATE SET
                        username = excluded.username,
                        nickname = excluded.nickname,
                        roles = excluded.roles
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

    def update_last_seen_and_get_previous(self, guild_id: int, user_id: int) -> Optional[int]:
        """Zero a member's last_seen (mark online) and return its previous value.

        Read-and-overwrite in a single transaction so the away duration — needed
        for the returning-member capture and the online-return watch alert — is
        never lost to the race between the two presence listeners. Returns the
        prior last_seen (a timestamp, or 0 if already online), or None if the
        member row does not exist. Replaces a get_member() + update_last_seen(0)
        pair with one lighter round-trip (no SELECT *, no roles JSON parse).
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT last_seen FROM members WHERE guild_id = ? AND user_id = ?",
                    (guild_id, user_id)
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                previous = row['last_seen']
                cursor.execute(
                    "UPDATE members SET last_seen = 0 WHERE guild_id = ? AND user_id = ?",
                    (guild_id, user_id)
                )
                return previous
        except Exception as e:
            logger.error(f"Failed to update/read last_seen for {user_id} in guild {guild_id}: {e}")
            return None

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

    def set_member_left_date(self, guild_id: int, user_id: int, timestamp: Optional[int]) -> bool:
        """Set (or clear, when timestamp is None) the date a member left the guild."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE members SET left_date = ? WHERE guild_id = ? AND user_id = ?
                """, (timestamp, guild_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to set left_date for {user_id} in guild {guild_id}: {e}")
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

    def add_member_return(self, guild_id: int, user_id: int, away_seconds: int, returned_at: int) -> bool:
        """Record that a member came back online after a long absence."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO member_returns (guild_id, user_id, away_seconds, returned_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (guild_id, user_id, away_seconds, returned_at)
                )
                return True
        except Exception as e:
            # Best-effort: a lost return record (e.g. member left before the flush,
            # tripping the FK) is not worth retrying. Log and drop.
            logger.error(f"Failed to record member return for {user_id} in guild {guild_id}: {e}")
            return False

    def count_returns(self, guild_id: int, since_ts: int) -> int:
        """Count returning members recorded for a guild since a timestamp."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM member_returns WHERE guild_id = ? AND returned_at >= ?",
                    (guild_id, since_ts)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to count returns for guild {guild_id}: {e}")
            return 0

    def get_recent_returns(self, guild_id: int, since_ts: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Recent returning members (most recent first), joined to current names.

        Only rows whose member still exists are returned (INNER JOIN); a member
        who has since left and been purged is dropped from the view.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT r.user_id, r.away_seconds, r.returned_at,
                           m.username, m.nickname
                    FROM member_returns r
                    JOIN members m
                      ON m.guild_id = r.guild_id AND m.user_id = r.user_id
                    WHERE r.guild_id = ? AND r.returned_at >= ?
                    ORDER BY r.returned_at DESC
                    LIMIT ?
                    """,
                    (guild_id, since_ts, limit)
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get recent returns for guild {guild_id}: {e}")
            return []

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

        Members never seen online (NULL last_seen) count as inactive once the
        threshold has passed since the bot could first observe them: the later
        of the guild's added_at and their join_date.

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
                    SELECT m.* FROM members m
                    JOIN guilds g ON g.guild_id = m.guild_id
                    WHERE m.guild_id = ?
                    AND m.is_active = 1
                    AND (
                        (m.last_seen IS NOT NULL AND m.last_seen != 0 AND m.last_seen <= ?)
                        OR (
                            m.last_seen IS NULL
                            AND MAX(COALESCE(m.join_date, 0), g.added_at) <= ?
                        )
                    )
                    ORDER BY m.last_seen ASC
                """, (guild_id, threshold, threshold))

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

    def get_tracked_user_ids(self, guild_id: int) -> Optional[List[int]]:
        """Return the user_ids of members matching the guild's track_only_roles.

        Members are stored regardless of the filter; the filter is applied at
        read time by the listing/reporting surfaces. Returns None when no filter
        is configured (every member is in scope). An empty list means a filter is
        set but no stored member currently has a matching role.
        """
        config = self.get_guild_config(guild_id)
        raw = config.get('track_only_roles') if config else None
        if not raw:
            return None
        try:
            allowed = set(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            return None
        if not allowed:
            return None

        tracked = []
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id, roles FROM members WHERE guild_id = ?", (guild_id,))
                for row in cursor.fetchall():
                    try:
                        member_roles = set(json.loads(row['roles'])) if row['roles'] else set()
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if member_roles & allowed:
                        tracked.append(row['user_id'])
        except Exception as e:
            logger.error(f"Failed to get tracked user ids for guild {guild_id}: {e}")
            return None
        return tracked

    @staticmethod
    def _member_filter_clause(user_ids: Optional[List[int]], column: str = "user_id") -> Tuple[str, list]:
        """Build an ' AND <column> IN (...)' fragment restricting to user_ids.

        None -> no restriction (empty fragment). Empty list -> ' AND 1=0' so a
        role filter that no member satisfies yields no rows. Returns (sql, params).
        """
        if user_ids is None:
            return "", []
        if not user_ids:
            return " AND 1=0", []
        placeholders = ",".join("?" * len(user_ids))
        return f" AND {column} IN ({placeholders})", list(user_ids)

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

    def member_is_missing(self, guild_id: int, user_id: int) -> bool:
        """Check that a member is confirmed absent from the database.

        Unlike member_exists(), a query failure returns False here, so True
        means "the row is definitely gone" — callers can drop work for the
        member without losing data to transient DB errors.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 1 FROM members WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                return cursor.fetchone() is None
        except Exception as e:
            logger.error(f"Failed to check if member {user_id} is missing in guild {guild_id}: {e}")
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

    def get_guild_stats(self, guild_id: int, user_ids: Optional[List[int]] = None) -> Dict[str, int]:
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
                fclause, fparams = self._member_filter_clause(user_ids)

                # Total members tracked
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members WHERE guild_id = ?{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                stats['total_members'] = result[0] if result else 0

                # Active members
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members WHERE guild_id = ? AND is_active = 1{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                stats['active_members'] = result[0] if result else 0

                # Inactive (left) members
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members WHERE guild_id = ? AND is_active = 0{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                stats['inactive_members'] = result[0] if result else 0

        except Exception as e:
            logger.error(f"Failed to get guild stats for {guild_id}: {e}")

        return stats

    def get_activity_stats(self, guild_id: int, user_ids: Optional[List[int]] = None) -> Dict[str, Any]:
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
                fclause, fparams = self._member_filter_clause(user_ids)

                # Currently online (last_seen = 0)
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1 AND last_seen = 0{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                stats['currently_online'] = result[0] if result else 0

                # Never seen offline (never tracked - last_seen IS NULL)
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1 AND last_seen IS NULL{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                stats['never_seen_offline'] = result[0] if result else 0

                # Offline within last hour
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?{fclause}
                """, (guild_id, hour_ago, *fparams))
                result = cursor.fetchone()
                stats['offline_1h'] = result[0] if result else 0

                # Offline within last 24 hours
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?{fclause}
                """, (guild_id, day_ago, *fparams))
                result = cursor.fetchone()
                stats['offline_24h'] = result[0] if result else 0

                # Offline within last 7 days
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?{fclause}
                """, (guild_id, week_ago, *fparams))
                result = cursor.fetchone()
                stats['offline_7d'] = result[0] if result else 0

                # Offline within last 30 days
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen >= ?{fclause}
                """, (guild_id, month_ago, *fparams))
                result = cursor.fetchone()
                stats['offline_30d'] = result[0] if result else 0

                # Offline more than 30 days
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                    AND last_seen > 0 AND last_seen < ?{fclause}
                """, (guild_id, month_ago, *fparams))
                result = cursor.fetchone()
                stats['offline_30d_plus'] = result[0] if result else 0

                # Total currently offline (total active - currently online)
                # Note: offline buckets above are overlapping (e.g., 2h offline counts in all buckets),
                # so we calculate it as: total active members - online members
                cursor.execute(f"""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1{fclause}
                """, (guild_id, *fparams))
                result = cursor.fetchone()
                total_active = result[0] if result else 0
                stats['currently_offline'] = total_active - stats['currently_online']

        except Exception as e:
            logger.error(f"Failed to get activity stats for {guild_id}: {e}")

        return stats

    # ---- Participation segments (lurkers / ghosts) ----
    #
    # These split active members by the gap between *presence* (last_seen) and
    # *participation* (message_activity), over a fixed window:
    #
    #   Lurker  = present but silent: seen within the window, yet zero messages
    #             in the window.
    #   Ghost   = dead weight: never sent a single message and not seen within
    #             the window (never tracked, or offline longer than the window).
    #
    # Both exclude members who joined *inside* the window, so brand-new members
    # aren't punished before they've had a chance to speak. Opted-out users never
    # appear: /forgetme purges their member row entirely (see purge_user_data).
    #
    # last_seen encoding (see get_activity_stats): 0 = online now,
    # NULL = never tracked, > 0 = unix time they last went offline.

    def _participation_window_start(self, window_days: int) -> int:
        """Start-of-day cutoff `window_days` ago, matching message_activity dates."""
        now = datetime.now(timezone.utc)
        today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
        return today_start - (window_days * SECONDS_PER_DAY)

    def get_participation_segments(self, guild_id: int, window_days: int = 30) -> Dict[str, Any]:
        """
        Count active members split into participants, lurkers, and ghosts.

        Cheap COUNT-only queries suitable for the stats overview. See the block
        comment above for the segment definitions.

        Returns:
            Dict with 'total_active', 'lurkers', 'ghosts', and 'lurker_pct'.
        """
        result = {'total_active': 0, 'lurkers': 0, 'ghosts': 0, 'lurker_pct': 0.0}
        try:
            window_start = self._participation_window_start(window_days)
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT COUNT(*) FROM members
                    WHERE guild_id = ? AND is_active = 1
                """, (guild_id,))
                result['total_active'] = cursor.fetchone()[0]

                # Lurker: established, seen within window, no messages in window.
                cursor.execute("""
                    SELECT COUNT(*) FROM members m
                    WHERE m.guild_id = ? AND m.is_active = 1
                      AND (m.join_date IS NULL OR m.join_date < ?)
                      AND (m.last_seen = 0 OR (m.last_seen IS NOT NULL AND m.last_seen >= ?))
                      AND NOT EXISTS (
                          SELECT 1 FROM message_activity ma
                          WHERE ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                            AND ma.date >= ?
                      )
                """, (guild_id, window_start, window_start, window_start))
                result['lurkers'] = cursor.fetchone()[0]

                # Ghost: established, not seen within window, never any message.
                cursor.execute("""
                    SELECT COUNT(*) FROM members m
                    WHERE m.guild_id = ? AND m.is_active = 1
                      AND (m.join_date IS NULL OR m.join_date < ?)
                      AND (m.last_seen IS NULL OR (m.last_seen > 0 AND m.last_seen < ?))
                      AND NOT EXISTS (
                          SELECT 1 FROM message_activity ma
                          WHERE ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                      )
                """, (guild_id, window_start, window_start))
                result['ghosts'] = cursor.fetchone()[0]

            if result['total_active'] > 0:
                result['lurker_pct'] = result['lurkers'] / result['total_active'] * 100
        except Exception as e:
            logger.error(f"Failed to get participation segments for guild {guild_id}: {e}")
        return result

    def get_lifecycle_segments(self, guild_id: int, new_days: int = 7,
                               active_window_days: int = 30, active_min: int = 5,
                               quiet_window_days: int = 90, return_days: int = 14) -> Dict[str, int]:
        """Classify active members into behavioural lifecycle stages.

        Hybrid model: combines the presence signal (members.last_seen) with the
        message signal (message_activity). Every active member lands in exactly
        one bucket, resolved by the precedence encoded in the CASE below —
        new → exploring → returned → active → quiet → dormant:

          🐣 new       — joined within new_days, never posted
          🌱 exploring — joined within new_days, has posted at least once
          🔄 returned  — reappeared online after a long absence (a member_returns
                         row within return_days); presence-based, not posting-based
          🔥 active    — >= active_min messages within active_window_days
          😴 quiet     — not active, but seen (presence) within active_window_days
                         or posted within quiet_window_days
          👻 dormant   — no recent presence and no recent posting

        Read-time only — writes nothing. One aggregate pass over this guild's
        message_activity (idx_message_activity_user_date) joined to members plus a
        small member_returns lookup, so it stays cheap enough for the stats view.

        Returns:
            Dict mapping each bucket name to its member count.
        """
        result = {'new': 0, 'exploring': 0, 'returned': 0, 'active': 0, 'quiet': 0, 'dormant': 0}
        try:
            now_ts = int(datetime.now(timezone.utc).timestamp())
            new_win = now_ts - new_days * SECONDS_PER_DAY
            ret_win = now_ts - return_days * SECONDS_PER_DAY
            active_win = self._participation_window_start(active_window_days)
            quiet_win = self._participation_window_start(quiet_window_days)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT
                        CASE
                            WHEN m.join_date >= ? AND agg.last_post IS NULL         THEN 'new'
                            WHEN m.join_date >= ?                                   THEN 'exploring'
                            WHEN ret.user_id IS NOT NULL                            THEN 'returned'
                            WHEN COALESCE(agg.msgs_recent, 0) >= ?                  THEN 'active'
                            WHEN (m.last_seen = 0 OR m.last_seen >= ?)
                              OR (agg.last_post IS NOT NULL AND agg.last_post >= ?) THEN 'quiet'
                            ELSE 'dormant'
                        END AS bucket,
                        COUNT(*)
                    FROM members m
                    LEFT JOIN (
                        SELECT user_id,
                               MAX(date) AS last_post,
                               SUM(CASE WHEN date >= ? THEN message_count ELSE 0 END) AS msgs_recent
                        FROM message_activity
                        WHERE guild_id = ?
                        GROUP BY user_id
                    ) agg ON agg.user_id = m.user_id
                    LEFT JOIN (
                        SELECT DISTINCT user_id FROM member_returns
                        WHERE guild_id = ? AND returned_at >= ?
                    ) ret ON ret.user_id = m.user_id
                    WHERE m.guild_id = ? AND m.is_active = 1
                    GROUP BY bucket
                """, (new_win, new_win, active_min, active_win, quiet_win,
                      active_win, guild_id, guild_id, ret_win, guild_id))
                for bucket, count in cursor.fetchall():
                    result[bucket] = count
        except Exception as e:
            logger.error(f"Failed to get lifecycle segments for guild {guild_id}: {e}")
        return result

    def get_lurkers(self, guild_id: int, window_days: int = 30, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Active members seen within the window but with zero messages in it.

        Ordered most-recently-seen first. See the participation block comment for
        the full definition.
        """
        try:
            window_start = self._participation_window_start(window_days)
            limit_clause = " LIMIT ?" if limit else ""
            params = [guild_id, window_start, window_start, window_start]
            if limit:
                params.append(limit)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT m.user_id, m.username, m.nickname, m.last_seen
                    FROM members m
                    WHERE m.guild_id = ? AND m.is_active = 1
                      AND (m.join_date IS NULL OR m.join_date < ?)
                      AND (m.last_seen = 0 OR (m.last_seen IS NOT NULL AND m.last_seen >= ?))
                      AND NOT EXISTS (
                          SELECT 1 FROM message_activity ma
                          WHERE ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                            AND ma.date >= ?
                      )
                    ORDER BY CASE WHEN m.last_seen = 0 THEN 1 ELSE 0 END DESC,
                             m.last_seen DESC{limit_clause}
                """, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get lurkers for guild {guild_id}: {e}")
            return []

    def get_ghosts(self, guild_id: int, window_days: int = 30, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Active members who never messaged and weren't seen within the window.

        Ordered by last seen ascending, which (since SQLite sorts NULL first)
        surfaces the most-gone first: never-tracked members, then those seen
        longest ago. See the participation block comment for the definition.
        """
        try:
            window_start = self._participation_window_start(window_days)
            limit_clause = " LIMIT ?" if limit else ""
            params = [guild_id, window_start, window_start]
            if limit:
                params.append(limit)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT m.user_id, m.username, m.nickname, m.last_seen
                    FROM members m
                    WHERE m.guild_id = ? AND m.is_active = 1
                      AND (m.join_date IS NULL OR m.join_date < ?)
                      AND (m.last_seen IS NULL OR (m.last_seen > 0 AND m.last_seen < ?))
                      AND NOT EXISTS (
                          SELECT 1 FROM message_activity ma
                          WHERE ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                      )
                    ORDER BY m.last_seen ASC{limit_clause}
                """, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get ghosts for guild {guild_id}: {e}")
            return []

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

                # Count records before deletion for logging
                cursor.execute("SELECT COUNT(*) FROM members WHERE guild_id = ?", (guild_id,))
                members_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM message_activity WHERE guild_id = ?", (guild_id,))
                activity_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM message_activity_hourly WHERE guild_id = ?", (guild_id,))
                hourly_count = cursor.fetchone()[0]

                # Deleting the guild cascades to members, role_changes,
                # message_activity, and message_activity_hourly automatically
                cursor.execute("DELETE FROM guilds WHERE guild_id = ?", (guild_id,))

                logger.info(
                    f"Removed guild {guild_id} from database. "
                    f"Deleted {members_count} members, "
                    f"{activity_count} daily and {hourly_count} hourly activity records."
                )
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

                # A row absent for this date means it's the member's first
                # message of a new active day — the transition the journey
                # summary folds on. Detected before the upsert creates the row.
                cursor.execute(
                    "SELECT 1 FROM message_activity WHERE guild_id = ? AND user_id = ? AND date = ?",
                    (guild_id, user_id, date)
                )
                is_new_day = cursor.fetchone() is None

                # INSERT OR REPLACE approach: if record exists, increment; if not, create with count
                cursor.execute("""
                    INSERT INTO message_activity (guild_id, user_id, date, message_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id, date) DO UPDATE SET
                    message_count = message_count + ?
                """, (guild_id, user_id, date, count, count))

                # Fold the same write into the durable journey rollup, in this
                # transaction, so the two never drift.
                self._apply_activity_summary(cursor, guild_id, user_id, date, count, is_new_day)

                return True
        except Exception as e:
            logger.error(f"Failed to increment message activity for user {user_id}: {e}")
            return False

    def _apply_activity_summary(self, cursor, guild_id: int, user_id: int,
                                date: int, count: int, is_new_day: bool) -> None:
        """Fold one message write into the member's journey rollup.

        `count` is always added to the lifetime total. When `is_new_day` is set
        (this is the member's first message on `date`), the active-day count and
        the streak/gap trackers advance too. Runs on the caller's cursor so it
        shares the message_activity transaction.
        """
        cursor.execute("""
            SELECT first_active, last_active, total_messages, active_days,
                   current_streak, longest_streak, longest_gap
            FROM member_activity_summary
            WHERE guild_id = ? AND user_id = ?
        """, (guild_id, user_id))
        row = cursor.fetchone()

        if row is None:
            # First message we've ever summarised for this member.
            cursor.execute("""
                INSERT INTO member_activity_summary
                    (guild_id, user_id, first_active, last_active, total_messages,
                     active_days, current_streak, longest_streak, longest_gap)
                VALUES (?, ?, ?, ?, ?, 1, 1, 1, 0)
            """, (guild_id, user_id, date, date, count))
            return

        first_active, last_active, total, days, cur, best, gap = row
        total += count

        if is_new_day:
            if last_active is None or date > last_active:
                # Normal forward move: measure the closed gap since the last
                # active day and extend or reset the current streak.
                closed = (date - last_active) // SECONDS_PER_DAY - 1 if last_active else 0
                if closed <= 0:
                    cur += 1
                else:
                    if closed > gap:
                        gap = closed
                    cur = 1
                if cur > best:
                    best = cur
                days += 1
                last_active = date
                if first_active is None:
                    first_active = date
            else:
                # Out-of-order older active day (rare, e.g. a buffer flushed
                # across midnight): count it, keep first_active earliest, but
                # don't try to recompute the streak.
                days += 1
                if first_active is None or date < first_active:
                    first_active = date

        cursor.execute("""
            UPDATE member_activity_summary
            SET first_active = ?, last_active = ?, total_messages = ?,
                active_days = ?, current_streak = ?, longest_streak = ?,
                longest_gap = ?
            WHERE guild_id = ? AND user_id = ?
        """, (first_active, last_active, total, days, cur, best, gap,
              guild_id, user_id))

    def get_member_journey(self, guild_id: int, user_id: int,
                           conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
        """Assemble the Member Journey view for one member.

        Combines the durable activity summary with the member's join_date /
        last_seen and their returning-member record. Returns None if the member
        isn't tracked at all; a tracked member who has never posted still comes
        back (with zeroed activity) so the join/last-seen lines can render.
        """
        try:
            with self._borrow(conn) as conn:
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT join_date, last_seen FROM members WHERE guild_id = ? AND user_id = ?",
                    (guild_id, user_id)
                )
                m = cursor.fetchone()
                if m is None:
                    return None

                cursor.execute("""
                    SELECT first_active, last_active, total_messages, active_days,
                           longest_streak, longest_gap
                    FROM member_activity_summary
                    WHERE guild_id = ? AND user_id = ?
                """, (guild_id, user_id))
                s = cursor.fetchone()

                cursor.execute(
                    "SELECT COUNT(*), MAX(away_seconds) FROM member_returns WHERE guild_id = ? AND user_id = ?",
                    (guild_id, user_id)
                )
                r = cursor.fetchone()

                return {
                    'join_date': m[0],
                    'last_seen': m[1],
                    'first_active': s[0] if s else None,
                    'last_active': s[1] if s else None,
                    'total_messages': s[2] if s else 0,
                    'active_days': s[3] if s else 0,
                    'longest_streak': s[4] if s else 0,
                    'longest_gap': s[5] if s else 0,
                    'return_count': r[0] if r else 0,
                    'longest_away_days': (r[1] // SECONDS_PER_DAY) if (r and r[1]) else None,
                }
        except Exception as e:
            logger.error(f"Failed to get member journey for user {user_id}: {e}")
            return None

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

    def get_activity_percentile(self, guild_id: int, user_id: int, days: int = 30,
                                conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
        """
        Rank a member's message activity against all active members over a period.

        The population is every active member (matching the leaderboard), with
        members who have no activity rows counted as 0 messages. "Percentile" is
        the share of *other* ranked members strictly less active than the caller,
        so ties do not inflate it. Rank is 1-based (1 = most active).

        Returns None when the population is too small to be meaningful
        (< MIN_RANKED members) or on error, so callers can simply skip the line.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            days: Look-back window in days (default 30)

        Returns:
            Dict with 'percentile' (0-100 int), 'rank', 'total_ranked',
            'caller_total', or None.
        """
        MIN_RANKED = 5
        try:
            # Match get_message_activity_period's cutoff exactly so the caller's
            # total here lines up with the "this month" figure shown alongside it.
            now = datetime.now(timezone.utc)
            today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
            cutoff_date = today_start - (days * SECONDS_PER_DAY)

            with self._borrow(conn) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND user_id = ? AND date >= ?
                """, (guild_id, user_id, cutoff_date))
                caller_total = cursor.fetchone()[0]

                # Per-member totals over the window (0 for members with no rows),
                # collapsed into counts above/below the caller in one pass.
                cursor.execute("""
                    SELECT
                        SUM(CASE WHEN total > ? THEN 1 ELSE 0 END) AS above,
                        SUM(CASE WHEN total < ? THEN 1 ELSE 0 END) AS below,
                        COUNT(*) AS total_ranked
                    FROM (
                        SELECT COALESCE(SUM(ma.message_count), 0) AS total
                        FROM members m
                        LEFT JOIN message_activity ma
                            ON ma.guild_id = m.guild_id
                            AND ma.user_id = m.user_id
                            AND ma.date >= ?
                        WHERE m.guild_id = ? AND m.is_active = 1
                        GROUP BY m.user_id
                    )
                """, (caller_total, caller_total, cutoff_date, guild_id))
                row = cursor.fetchone()

            if not row or not row['total_ranked'] or row['total_ranked'] < MIN_RANKED:
                return None

            above = row['above'] or 0
            below = row['below'] or 0
            total_ranked = row['total_ranked']

            # Denominator excludes the caller themselves.
            percentile = round(below / (total_ranked - 1) * 100) if total_ranked > 1 else 0

            return {
                'percentile': percentile,
                'rank': above + 1,
                'total_ranked': total_ranked,
                'caller_total': caller_total,
            }
        except Exception as e:
            logger.error(f"Failed to get activity percentile for user {user_id} in guild {guild_id}: {e}")
            return None

    def get_message_activity_trend(self, guild_id: int, user_id: int, days: int = 365,
                                   conn: Optional[sqlite3.Connection] = None) -> List[Dict[str, Any]]:
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
            with self._borrow(conn) as conn:
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

    def get_activity_profile(self, guild_id: int, user_id: int, days: int = 30, tz_str: str = 'UTC',
                             conn: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
        """Build a per-member activity profile from message activity (never presence).

        Aggregates the member's hourly message buckets over the window into
        weekday and hour-of-day distributions, the count of distinct active
        days, and a coarse rising/steady/falling trend. Every bucket is
        converted to the guild's timezone per-row (not by a fixed offset), so
        DST and sub-hour offsets are handled correctly.

        This reads message_activity_hourly only — it does not touch last_seen or
        any presence data, keeping the single-column presence commitment intact.

        Args:
            guild_id: Discord guild ID
            user_id: Discord user ID
            days: Look-back window in days (default 30)
            tz_str: Guild timezone name for local-time bucketing (default UTC)

        Returns:
            Dict with 'total', 'active_days', 'window_days', 'by_weekday'
            (0=Mon..6=Sun -> count), 'by_hour' (0-23 -> count), 'peak_weekday'
            (0-6 or None), 'peak_hours' ((start_hour, end_hour) or None), and
            'trend' ('increasing'|'steady'|'decreasing'|None).
        """
        import pytz

        empty = {
            'total': 0, 'active_days': 0, 'window_days': days,
            'by_weekday': {i: 0 for i in range(7)},
            'by_hour': {h: 0 for h in range(24)},
            'peak_weekday': None, 'peak_hours': None, 'trend': None,
        }
        try:
            try:
                tz = pytz.timezone(tz_str) if tz_str in pytz.all_timezones else pytz.UTC
            except Exception:
                tz = pytz.UTC

            now = int(datetime.now(timezone.utc).timestamp())
            period_start = now - (days * SECONDS_PER_DAY)

            with self._borrow(conn) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp, message_count
                    FROM message_activity_hourly
                    WHERE guild_id = ? AND user_id = ? AND timestamp >= ?
                """, (guild_id, user_id, period_start))
                rows = cursor.fetchall()

            if not rows:
                return empty

            by_weekday = {i: 0 for i in range(7)}
            by_hour = {h: 0 for h in range(24)}
            per_date: Dict[Any, int] = {}
            total = 0

            for row in rows:
                ts = row['timestamp']
                cnt = row['message_count'] or 0
                local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)
                by_weekday[local.weekday()] += cnt
                by_hour[local.hour] += cnt
                d = local.date()
                per_date[d] = per_date.get(d, 0) + cnt
                total += cnt

            if total == 0:
                return empty

            peak_weekday = max(by_weekday, key=by_weekday.get)

            # Peak activity band: the 4-hour window (wrap-safe across midnight)
            # with the most messages. Answers "when are they most concentrated".
            best_start, best_sum = 0, -1
            for start in range(24):
                s = sum(by_hour[(start + k) % 24] for k in range(4))
                if s > best_sum:
                    best_sum, best_start = s, start
            peak_hours = (best_start, (best_start + 4) % 24)

            # Coarse trend: recent half of the window vs the prior half, by
            # local date. Deliberately blunt — callers gate it behind a minimum
            # sample so it isn't reported off a handful of messages.
            half = days // 2
            today_local = datetime.now(tz).date()
            recent = prior = 0
            for d, c in per_date.items():
                age = (today_local - d).days
                if age < half:
                    recent += c
                elif age < days:
                    prior += c

            trend = None
            if recent + prior > 0:
                if prior == 0:
                    trend = 'increasing'
                elif recent >= prior * 1.2:
                    trend = 'increasing'
                elif recent <= prior * 0.8:
                    trend = 'decreasing'
                else:
                    trend = 'steady'

            return {
                'total': total, 'active_days': len(per_date), 'window_days': days,
                'by_weekday': by_weekday, 'by_hour': by_hour,
                'peak_weekday': peak_weekday, 'peak_hours': peak_hours, 'trend': trend,
            }
        except Exception as e:
            logger.error(f"Failed to build activity profile for user {user_id} in guild {guild_id}: {e}")
            return empty

    def get_whois_activity_panel(self, guild_id: int, user_id: int, days: int = 30,
                                 tz_str: str = 'UTC') -> Dict[str, Any]:
        """Bundle the three per-member activity reads /whois needs into one round-trip.

        Runs the profile, the daily trend (for the sparkline) and — only when the
        profile clears the same thin-data gate the embed uses — the percentile,
        all on a single borrowed connection instead of three. Returns
        {'profile', 'trend', 'percentile'} with the same shapes the individual
        methods return ('percentile' is None when skipped or unavailable).
        """
        with self._borrow(None) as conn:
            profile = self.get_activity_profile(guild_id, user_id, days, tz_str, conn=conn)
            trend = self.get_message_activity_trend(guild_id, user_id, days, conn=conn)
            # Match the embed's gate so the population-wide percentile query is
            # skipped on thin data exactly as before.
            if profile['total'] >= 15 and profile['active_days'] >= 5:
                percentile = self.get_activity_percentile(guild_id, user_id, days, conn=conn)
            else:
                percentile = None
        return {'profile': profile, 'trend': trend, 'percentile': percentile}

    def get_guild_message_activity_stats(self, guild_id: int, days: int = 365, user_ids: Optional[List[int]] = None) -> Dict[str, Any]:
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
                # Restrict every aggregate to the guild's track_only_roles filter
                # (read-time). Empty fragment when no filter is set.
                fclause, fparams = self._member_filter_clause(user_ids)

                # Get today's date (start of day UTC)
                now = datetime.now(timezone.utc)
                today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())

                # Calculate cutoff dates
                cutoff_date = today_start - (days * SECONDS_PER_DAY)
                week_cutoff = today_start - (7 * SECONDS_PER_DAY)
                month_cutoff = today_start - (30 * SECONDS_PER_DAY)
                quarter_cutoff = today_start - (90 * SECONDS_PER_DAY)

                # Get total messages for all periods
                cursor.execute(f"""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                """, (guild_id, cutoff_date, *fparams))
                total_365d = cursor.fetchone()[0]

                # Get 90-day total
                cursor.execute(f"""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                """, (guild_id, quarter_cutoff, *fparams))
                total_90d = cursor.fetchone()[0]

                # Get 30-day total
                cursor.execute(f"""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                """, (guild_id, month_cutoff, *fparams))
                total_30d = cursor.fetchone()[0]

                # Get 7-day total
                cursor.execute(f"""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                """, (guild_id, week_cutoff, *fparams))
                total_7d = cursor.fetchone()[0]

                # Get today's count
                cursor.execute(f"""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE guild_id = ? AND date = ?{fclause}
                """, (guild_id, today_start, *fparams))
                today_count = cursor.fetchone()[0]

                # Get busiest and quietest days (365 days)
                cursor.execute(f"""
                    SELECT date, SUM(message_count) as total
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                    GROUP BY date
                    ORDER BY total DESC
                    LIMIT 1
                """, (guild_id, cutoff_date, *fparams))
                busiest_day = cursor.fetchone()

                cursor.execute(f"""
                    SELECT date, SUM(message_count) as total
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                    GROUP BY date
                    ORDER BY total ASC
                    LIMIT 1
                """, (guild_id, cutoff_date, *fparams))
                quietest_day = cursor.fetchone()

                # Get active member count (30 days)
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT user_id)
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                """, (guild_id, month_cutoff, *fparams))
                active_members_30d = cursor.fetchone()[0]
                
                # Average per day over the requested period. total_365d holds the
                # sum over `days` days (cutoff_date uses `days`), so divide by
                # `days`, not a fixed 365 — otherwise weekly/monthly reports show
                # ~1/52 and ~1/12 of the true daily average.
                avg_per_day = round(total_365d / days, 1) if total_365d > 0 else 0
                
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
                
                # Get when bot was added to guild to filter out historical joins/leaves
                cursor.execute("""
                    SELECT added_at FROM guilds WHERE guild_id = ?
                """, (guild_id,))
                result = cursor.fetchone()
                bot_added_at = result['added_at'] if result else 0
                
                # Count joins this month (only after bot was added)
                cursor.execute("""
                    SELECT COUNT(*) as joins
                    FROM members
                    WHERE guild_id = ? AND join_date >= ? AND join_date >= ?
                """, (guild_id, month_start, bot_added_at))
                joins_this_month = cursor.fetchone()['joins'] or 0
                
                # Count members who left this month (only after bot was added)
                cursor.execute("""
                    SELECT COUNT(*) as leaves
                    FROM members
                    WHERE guild_id = ? AND is_active = 0 AND last_seen >= ? AND last_seen >= ?
                """, (guild_id, month_start, bot_added_at))
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
                
                # Get when bot was added to guild to filter out historical joins
                cursor.execute("""
                    SELECT added_at FROM guilds WHERE guild_id = ?
                """, (guild_id,))
                result = cursor.fetchone()
                bot_added_at = result['added_at'] if result else 0
                
                # Members who joined in this period (but only after the bot was added)
                cursor.execute("""
                    SELECT COUNT(*) as joins
                    FROM members
                    WHERE guild_id = ? AND join_date >= ? AND join_date >= ?
                """, (guild_id, period_start, bot_added_at))
                joins = cursor.fetchone()['joins'] or 0
                
                # Members who left in this period (is_active = 0 and last_seen >= period_start)
                # Only count leaves after bot was added
                cursor.execute("""
                    SELECT COUNT(*) as leaves
                    FROM members
                    WHERE guild_id = ? AND is_active = 0 AND last_seen >= ? AND last_seen >= ?
                """, (guild_id, period_start, bot_added_at))
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

    # Windowed activation eras as (label, first_day_offset, last_day_offset) from
    # the member's join day, inclusive and non-overlapping. Each checkpoint asks
    # "did they post *during* this stretch?" — the still-around reading, not a
    # cumulative "ever posted by day N".
    _ACTIVATION_ERAS = (('D1', 0, 1), ('D7', 2, 7), ('D30', 8, 30), ('D90', 31, 90))
    # Below this many recent joiners the percentages are too noisy to show.
    _ACTIVATION_MIN_COHORT = 5

    def get_activation_funnel(self, guild_id: int) -> Dict[str, Any]:
        """
        Windowed new-member activation funnel built from message activity.

        For members who joined within the guild's message-retention horizon,
        measures the share who *posted a message* during each successive era
        after joining (see _ACTIVATION_ERAS). Each checkpoint's denominator is
        only members old enough to have lived through the end of that era
        ("matured"); bounding the cohort to join_date within the retention
        window guarantees no era's activity rows have been pruned, so the curve
        is unbiased. message_activity is the only signal here — a lurker who
        never posts reads as not activated, by design.

        Returns:
            {
              'cohort_size': int,                 # joiners inside the window
              'checkpoints': [                    # only eras with matured members
                  {'label': str, 'active': int, 'matured': int, 'rate': float}, ...
              ],
              'largest_dropoff': (from_label, to_label) | None,
            }
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())

                cursor.execute(
                    "SELECT message_retention_days FROM guilds WHERE guild_id = ?",
                    (guild_id,),
                )
                row = cursor.fetchone()
                retention_days = (row['message_retention_days'] if row and row['message_retention_days'] else 365)
                cutoff = now - retention_days * SECONDS_PER_DAY

                # Cohort: joiners inside the retention window. join_date floored to
                # its UTC day-start so it lines up with message_activity.date
                # (which is stored as start-of-day UTC).
                cursor.execute("""
                    SELECT user_id, join_date FROM members
                    WHERE guild_id = ? AND join_date IS NOT NULL AND join_date >= ?
                """, (guild_id, cutoff))
                join_day: Dict[int, int] = {}
                for r in cursor.fetchall():
                    jd = r['join_date']
                    join_day[r['user_id']] = jd - (jd % SECONDS_PER_DAY)

                cohort_size = len(join_day)
                empty = {'cohort_size': cohort_size, 'checkpoints': [], 'largest_dropoff': None}
                if cohort_size < self._ACTIVATION_MIN_COHORT:
                    return empty

                # Every activity day for cohort members, in one pass.
                cursor.execute("""
                    SELECT ma.user_id, ma.date
                    FROM message_activity ma
                    JOIN members m ON m.guild_id = ma.guild_id AND m.user_id = ma.user_id
                    WHERE ma.guild_id = ? AND m.join_date >= ? AND ma.date >= ?
                """, (guild_id, cutoff, cutoff))
                activity: Dict[int, list] = defaultdict(list)
                for r in cursor.fetchall():
                    activity[r['user_id']].append(r['date'])

                active = {label: 0 for label, _, _ in self._ACTIVATION_ERAS}
                matured = {label: 0 for label, _, _ in self._ACTIVATION_ERAS}
                for user_id, jday in join_day.items():
                    dates = activity.get(user_id, ())
                    for label, ws, we in self._ACTIVATION_ERAS:
                        win_start = jday + ws * SECONDS_PER_DAY
                        win_end = jday + we * SECONDS_PER_DAY
                        if now < win_end:  # era not finished yet — not matured
                            continue
                        matured[label] += 1
                        if any(win_start <= d <= win_end for d in dates):
                            active[label] += 1

                checkpoints = []
                for label, _, _ in self._ACTIVATION_ERAS:
                    if matured[label] > 0:
                        checkpoints.append({
                            'label': label,
                            'active': active[label],
                            'matured': matured[label],
                            'rate': active[label] / matured[label] * 100,
                        })

                largest_dropoff = None
                worst = 0.0
                for prev, cur in zip(checkpoints, checkpoints[1:]):
                    drop = prev['rate'] - cur['rate']
                    if drop > worst:
                        worst = drop
                        largest_dropoff = (prev['label'], cur['label'])

                return {
                    'cohort_size': cohort_size,
                    'checkpoints': checkpoints,
                    'largest_dropoff': largest_dropoff,
                }
        except Exception as e:
            logger.error(f"Failed to get activation funnel for guild {guild_id}: {e}")
            return {'cohort_size': 0, 'checkpoints': [], 'largest_dropoff': None}

    def get_departure_lifespan(self, guild_id: int) -> Dict[str, Any]:
        """
        Analyse how long departed members stayed before leaving.

        Tenure is left_date - join_date. Only departures recorded since the
        left_date column was added are analysable (older ones have left_date
        NULL); rejoining clears left_date, so only the current departure counts.
        The churn side of retention — pairs with get_retention_cohorts().

        Returns a dict with 'sample' (number of analysable departures), and when
        sample >= MIN_SAMPLE: 'median_days', 'avg_days', 'buckets' (non-overlapping
        tenure ranges), and 'early_churn_pct' (share who left within a week).
        Below MIN_SAMPLE the stats stay zeroed so callers can show a placeholder.
        """
        MIN_SAMPLE = 3
        EARLY_CHURN_DAYS = 7
        result = {
            'sample': 0,
            'median_days': 0,
            'avg_days': 0,
            'buckets': {'under_1d': 0, 'under_7d': 0, 'under_30d': 0, 'under_90d': 0, 'over_90d': 0},
            'early_churn_pct': 0.0,
        }
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT (left_date - join_date) AS tenure
                    FROM members
                    WHERE guild_id = ? AND is_active = 0
                      AND left_date IS NOT NULL AND join_date IS NOT NULL
                      AND left_date > join_date
                """, (guild_id,))
                tenures = sorted(row['tenure'] for row in cursor.fetchall())

            n = len(tenures)
            result['sample'] = n
            if n < MIN_SAMPLE:
                return result

            # Median in seconds (average the two middle values for even counts).
            mid = n // 2
            median_s = tenures[mid] if n % 2 else (tenures[mid - 1] + tenures[mid]) / 2
            result['median_days'] = round(median_s / SECONDS_PER_DAY)
            result['avg_days'] = round(sum(tenures) / n / SECONDS_PER_DAY)

            d = SECONDS_PER_DAY
            b = result['buckets']
            for tsec in tenures:
                if tsec < 1 * d:
                    b['under_1d'] += 1
                elif tsec < 7 * d:
                    b['under_7d'] += 1
                elif tsec < 30 * d:
                    b['under_30d'] += 1
                elif tsec < 90 * d:
                    b['under_90d'] += 1
                else:
                    b['over_90d'] += 1

            early = sum(1 for tsec in tenures if tsec < EARLY_CHURN_DAYS * d)
            result['early_churn_pct'] = early / n * 100
            return result
        except Exception as e:
            logger.error(f"Failed to get departure lifespan for guild {guild_id}: {e}")
            return result

    # Deltas only make sense once the prior window is fully covered by data,
    # so weekly comparisons need 2x7 days of history and monthly ones 2x30.
    _HEALTH_WEEK_HISTORY_DAYS = 14
    _HEALTH_MONTH_HISTORY_DAYS = 60

    def get_server_health(self, guild_id: int) -> Dict[str, Any]:
        """
        Server vitals for the health panel: each metric for the current window
        and the immediately preceding one, so the caller can render deltas.
        No scoring — just the raw window-vs-window numbers.

        Message-derived windows (posters, messages, breadth, activation) are
        aligned to complete UTC days ending at the last UTC midnight, so both
        windows are the same length and today's partial day never skews the
        comparison. Event-derived windows (joins, leaves, returns) use raw
        timestamps back from now.

        Returns {} on error. Otherwise:
            days_of_data        days since the bot was added to the guild
            weekly_comparable   prior 7d window fully covered by data
            monthly_comparable  prior 30d window fully covered by data
            posters_7d / posters_prev_7d       distinct posters per week window
            messages_7d / messages_prev_7d     message totals per week window
            posters_30d / posters_prev_30d     distinct posters per 30d window
            messages_30d / messages_prev_30d   message totals per 30d window
            active_members                     current tracked member count
            joins_30d / joins_prev_30d         joins per 30d window
            leaves_30d / leaves_prev_30d       departures per 30d window
            returns_30d / returns_prev_30d     30+ day comebacks per window
            activation_cur / activation_prev   {'matured', 'activated', 'rate'}
                for month-apart join cohorts (posted within 7 days of joining),
                or None when the cohort is too small / data can't cover it
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = int(datetime.now(timezone.utc).timestamp())
                now_dt = datetime.now(timezone.utc)
                today_start = int(datetime(now_dt.year, now_dt.month, now_dt.day, tzinfo=timezone.utc).timestamp())
                d = SECONDS_PER_DAY

                cursor.execute("""
                    SELECT added_at, message_retention_days FROM guilds WHERE guild_id = ?
                """, (guild_id,))
                row = cursor.fetchone()
                added_at = row['added_at'] if row else now
                retention_days = (row['message_retention_days'] if row and row['message_retention_days'] else 365)
                days_of_data = max(0, (now - added_at) // d)

                result: Dict[str, Any] = {
                    'days_of_data': days_of_data,
                    'weekly_comparable': added_at <= today_start - self._HEALTH_WEEK_HISTORY_DAYS * d,
                    'monthly_comparable': added_at <= now - self._HEALTH_MONTH_HISTORY_DAYS * d,
                }

                # Distinct posters and message totals per day-aligned window.
                def message_window(start: int, end: int) -> tuple:
                    cursor.execute("""
                        SELECT COUNT(DISTINCT user_id) AS posters,
                               COALESCE(SUM(message_count), 0) AS messages
                        FROM message_activity
                        WHERE guild_id = ? AND date >= ? AND date < ?
                    """, (guild_id, start, end))
                    r = cursor.fetchone()
                    return r['posters'], r['messages']

                result['posters_7d'], result['messages_7d'] = message_window(today_start - 7 * d, today_start)
                result['posters_prev_7d'], result['messages_prev_7d'] = message_window(today_start - 14 * d, today_start - 7 * d)
                result['posters_30d'], result['messages_30d'] = message_window(today_start - 30 * d, today_start)
                result['posters_prev_30d'], result['messages_prev_30d'] = message_window(today_start - 60 * d, today_start - 30 * d)

                cursor.execute("""
                    SELECT COUNT(*) AS n FROM members WHERE guild_id = ? AND is_active = 1
                """, (guild_id,))
                result['active_members'] = cursor.fetchone()['n'] or 0

                def count_window(sql: str, start: int, end: int) -> int:
                    cursor.execute(sql, (guild_id, start, end))
                    return cursor.fetchone()['n'] or 0

                joins_sql = """
                    SELECT COUNT(*) AS n FROM members
                    WHERE guild_id = ? AND join_date >= ? AND join_date < ?
                """
                leaves_sql = """
                    SELECT COUNT(*) AS n FROM members
                    WHERE guild_id = ? AND is_active = 0
                      AND left_date IS NOT NULL AND left_date >= ? AND left_date < ?
                """
                returns_sql = """
                    SELECT COUNT(*) AS n FROM member_returns
                    WHERE guild_id = ? AND returned_at >= ? AND returned_at < ?
                """
                result['joins_30d'] = count_window(joins_sql, now - 30 * d, now + 1)
                result['joins_prev_30d'] = count_window(joins_sql, now - 60 * d, now - 30 * d)
                result['leaves_30d'] = count_window(leaves_sql, now - 30 * d, now + 1)
                result['leaves_prev_30d'] = count_window(leaves_sql, now - 60 * d, now - 30 * d)
                result['returns_30d'] = count_window(returns_sql, now - 30 * d, now + 1)
                result['returns_prev_30d'] = count_window(returns_sql, now - 60 * d, now - 30 * d)

                # First-week activation for two month-apart join cohorts. A
                # cohort member is "matured" once their first 7 days are over
                # (join_date < now - 7d). join_date >= added_at keeps out
                # members whose first week predates message tracking, and the
                # retention check guarantees the older cohort's activity rows
                # haven't been pruned.
                def activation_cohort(start: int, end: int) -> Optional[Dict[str, Any]]:
                    if start < now - retention_days * d:
                        return None
                    cursor.execute("""
                        SELECT COUNT(*) AS matured,
                               COALESCE(SUM(CASE WHEN EXISTS (
                                   SELECT 1 FROM message_activity ma
                                   WHERE ma.guild_id = m.guild_id AND ma.user_id = m.user_id
                                     AND ma.date >= (m.join_date - m.join_date % ?)
                                     AND ma.date <= (m.join_date - m.join_date % ?) + 7 * ?
                               ) THEN 1 ELSE 0 END), 0) AS activated
                        FROM members m
                        WHERE m.guild_id = ? AND m.join_date >= ? AND m.join_date < ? AND m.join_date >= ?
                    """, (d, d, d, guild_id, start, end, added_at))
                    r = cursor.fetchone()
                    if r['matured'] < self._ACTIVATION_MIN_COHORT:
                        return None
                    return {
                        'matured': r['matured'],
                        'activated': r['activated'],
                        'rate': r['activated'] / r['matured'] * 100,
                    }

                result['activation_cur'] = activation_cohort(now - 37 * d, now - 7 * d)
                result['activation_prev'] = activation_cohort(now - 67 * d, now - 37 * d)

                return result
        except Exception as e:
            logger.error(f"Failed to get server health for guild {guild_id}: {e}")
            return {}

    def record_health_snapshots(self) -> int:
        """
        Write one health snapshot per guild for the UTC day that just ended.

        Every count covers the day-aligned rolling window [midnight-7d,
        midnight), so re-running any time during the same UTC day produces the
        same row — INSERT OR REPLACE on (guild_id, date) makes the task safe to
        run at startup and again on its daily tick. Days the bot was fully
        offline are simply absent; get_health_history() tolerates the gaps.

        Returns the number of guilds snapshotted.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now_dt = datetime.now(timezone.utc)
                day = int(datetime(now_dt.year, now_dt.month, now_dt.day, tzinfo=timezone.utc).timestamp())
                start = day - 7 * SECONDS_PER_DAY

                cursor.execute("SELECT guild_id FROM guilds")
                guild_ids = [row['guild_id'] for row in cursor.fetchall()]

                for guild_id in guild_ids:
                    cursor.execute("""
                        SELECT COUNT(DISTINCT user_id) AS posters,
                               COALESCE(SUM(message_count), 0) AS messages
                        FROM message_activity
                        WHERE guild_id = ? AND date >= ? AND date < ?
                    """, (guild_id, start, day))
                    row = cursor.fetchone()
                    posters, messages = row['posters'], row['messages']

                    cursor.execute("""
                        SELECT COUNT(*) AS n FROM members WHERE guild_id = ? AND is_active = 1
                    """, (guild_id,))
                    active_members = cursor.fetchone()['n'] or 0

                    cursor.execute("""
                        SELECT COUNT(*) AS n FROM members
                        WHERE guild_id = ? AND join_date >= ? AND join_date < ?
                    """, (guild_id, start, day))
                    joins = cursor.fetchone()['n'] or 0

                    cursor.execute("""
                        SELECT COUNT(*) AS n FROM members
                        WHERE guild_id = ? AND is_active = 0
                          AND left_date IS NOT NULL AND left_date >= ? AND left_date < ?
                    """, (guild_id, start, day))
                    leaves = cursor.fetchone()['n'] or 0

                    cursor.execute("""
                        SELECT COUNT(*) AS n FROM member_returns
                        WHERE guild_id = ? AND returned_at >= ? AND returned_at < ?
                    """, (guild_id, start, day))
                    returns = cursor.fetchone()['n'] or 0

                    cursor.execute("""
                        INSERT OR REPLACE INTO health_snapshots
                            (guild_id, date, active_members, posters_7d, messages_7d,
                             joins_7d, leaves_7d, returns_7d)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (guild_id, day, active_members, posters, messages, joins, leaves, returns))

                conn.commit()
                return len(guild_ids)
        except Exception as e:
            logger.error(f"Failed to record health snapshots: {e}")
            return 0

    def get_health_history(self, guild_id: int, points: int = 12, step_days: int = 7) -> List[Dict[str, Any]]:
        """
        Sample health snapshots at step_days intervals for trend charting.

        Walks back from the newest snapshot, picking the first available row at
        or before each step so days the bot was offline are skipped rather than
        breaking the series. Returns up to `points` rows, oldest first.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT date, active_members, posters_7d, messages_7d,
                           joins_7d, leaves_7d, returns_7d
                    FROM health_snapshots
                    WHERE guild_id = ?
                    ORDER BY date DESC
                    LIMIT ?
                """, (guild_id, points * step_days))
                rows = [dict(row) for row in cursor.fetchall()]

            if not rows:
                return []

            selected = [rows[0]]
            target = rows[0]['date'] - step_days * SECONDS_PER_DAY
            for row in rows[1:]:
                if len(selected) >= points:
                    break
                if row['date'] <= target:
                    selected.append(row)
                    target = row['date'] - step_days * SECONDS_PER_DAY
            return list(reversed(selected))
        except Exception as e:
            logger.error(f"Failed to get health history for guild {guild_id}: {e}")
            return []

    def prune_old_health_snapshots(self, retention_days: int = HEALTH_SNAPSHOT_RETENTION_DAYS) -> int:
        """
        Delete health_snapshots older than retention_days.

        Rows older than the retention horizon are never read (see
        get_health_history), so removing them caps the table's daily growth
        with no effect on the trend chart. Returns the number of rows deleted.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now_dt = datetime.now(timezone.utc)
                day = int(datetime(now_dt.year, now_dt.month, now_dt.day, tzinfo=timezone.utc).timestamp())
                cutoff = day - retention_days * SECONDS_PER_DAY
                cursor.execute("DELETE FROM health_snapshots WHERE date < ?", (cutoff,))
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Failed to prune old health snapshots: {e}")
            return 0

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

    def get_activity_by_day(self, guild_id: int, days: int = 30, user_ids: Optional[List[int]] = None, tz_str: str = 'UTC') -> Dict[str, int]:
        """
        Get message activity distribution by day of week.

        Args:
            guild_id: Guild ID
            days: Number of days to look back
            user_ids: Optional member filter (track_only_roles scope)
            tz_str: Timezone name for local-day bucketing (default UTC), so the
                weekday matches the guild's own clock like the /user-stats heatmap

        Returns:
            Dictionary mapping day name to message count
        """
        import pytz
        try:
            try:
                tz = pytz.timezone(tz_str) if tz_str in pytz.all_timezones else pytz.UTC
            except Exception:
                tz = pytz.UTC

            with self.get_connection() as conn:
                cursor = conn.cursor()
                fclause, fparams = self._member_filter_clause(user_ids)
                now = int(datetime.now(timezone.utc).timestamp())
                period_start = now - (days * SECONDS_PER_DAY)

                cursor.execute(f"""
                    SELECT date, SUM(message_count) as count
                    FROM message_activity
                    WHERE guild_id = ? AND date >= ?{fclause}
                    GROUP BY date
                """, (guild_id, period_start, *fparams))

                day_counts = {'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0,
                             'Friday': 0, 'Saturday': 0, 'Sunday': 0}

                for row in cursor.fetchall():
                    date_dt = datetime.fromtimestamp(row['date'], tz=timezone.utc).astimezone(tz)
                    day_name = date_dt.strftime('%A')
                    day_counts[day_name] += row['count'] or 0

                return day_counts
        except Exception as e:
            logger.error(f"Failed to get activity by day for guild {guild_id}: {e}")
            return {}

    def get_server_activity_windows(self, guild_id: int, days: int = 30, tz_str: str = 'UTC') -> Dict[str, Any]:
        """Aggregate guild-wide message activity into local-time weekday/hour windows.

        Reads message_activity_hourly only (never presence), re-bucketing every
        hourly record into the guild's timezone per-row so DST and sub-hour
        offsets are handled correctly. The local weekday and hour are derived
        from each row's timestamp rather than the stored UTC hour column —
        required for an actionable "best time to reach the community"
        recommendation in the guild's own clock.

        Args:
            guild_id: Discord guild ID
            days: Look-back window in days (default 30)
            tz_str: Guild timezone name for local-time bucketing (default UTC)

        Returns:
            Dict with 'total', 'active_days', 'by_weekday' (0=Mon..6=Sun ->
            count), 'by_hour' (0-23 -> count), 'best_weekday' (0-6 or None),
            'peak_hour' (0-23 or None), 'recommend' and 'quiet'. The latter two
            are (weekday, start_hour, end_hour) tuples or None, with an
            exclusive end hour — e.g. (5, 19, 22) means Saturday 19:00-22:00.
            All hours are local to tz_str.
        """
        import pytz
        BAND = 3  # width in hours of the recommended / quietest window

        empty = {
            'total': 0, 'active_days': 0,
            'by_weekday': {i: 0 for i in range(7)},
            'by_hour': {h: 0 for h in range(24)},
            'best_weekday': None, 'peak_hour': None,
            'recommend': None, 'quiet': None,
        }
        try:
            try:
                tz = pytz.timezone(tz_str) if tz_str in pytz.all_timezones else pytz.UTC
            except Exception:
                tz = pytz.UTC

            now = int(datetime.now(timezone.utc).timestamp())
            period_start = now - (days * SECONDS_PER_DAY)

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp, message_count
                    FROM message_activity_hourly
                    WHERE guild_id = ? AND timestamp >= ?
                """, (guild_id, period_start))
                rows = cursor.fetchall()

            if not rows:
                return empty

            by_weekday = {i: 0 for i in range(7)}
            by_hour = {h: 0 for h in range(24)}
            grid = {(d, h): 0 for d in range(7) for h in range(24)}
            active_dates = set()
            total = 0

            for row in rows:
                cnt = row['message_count'] or 0
                if cnt <= 0:
                    continue
                local = datetime.fromtimestamp(row['timestamp'], tz=timezone.utc).astimezone(tz)
                d, h = local.weekday(), local.hour
                by_weekday[d] += cnt
                by_hour[h] += cnt
                grid[(d, h)] += cnt
                active_dates.add(local.date())
                total += cnt

            if total == 0:
                return empty

            best_weekday = max(by_weekday, key=by_weekday.get)
            peak_hour = max(by_hour, key=by_hour.get)

            # Busiest / quietest contiguous BAND-hour window on a single weekday.
            # Non-wrapping (start capped at 24 - BAND) so a window never straddles
            # midnight into a different weekday, keeping the recommendation literal.
            best_band = None
            best_sum = -1
            quiet_band = None
            quiet_sum = None
            for d in range(7):
                for start in range(0, 24 - BAND + 1):
                    s = sum(grid[(d, start + k)] for k in range(BAND))
                    if s > best_sum:
                        best_sum, best_band = s, (d, start, start + BAND)
                    if quiet_sum is None or s < quiet_sum:
                        quiet_sum, quiet_band = s, (d, start, start + BAND)

            return {
                'total': total, 'active_days': len(active_dates),
                'by_weekday': by_weekday, 'by_hour': by_hour,
                'best_weekday': best_weekday, 'peak_hour': peak_hour,
                'recommend': best_band, 'quiet': quiet_band,
            }
        except Exception as e:
            logger.error(f"Failed to build server activity windows for guild {guild_id}: {e}")
            return empty

    # ==================== Scheduled Reports Operations ====================

    def get_guilds_with_reports_enabled(self) -> list:
        """Get all guilds that have scheduled reports enabled."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT guild_id, report_channel_id, report_frequency, report_types,
                           report_day_weekly, report_day_monthly, last_weekly_report, last_monthly_report,
                           timezone, report_time_hour
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

    def get_top_active_users_period(self, guild_id: int, days: int, limit: int = 10, user_ids: Optional[List[int]] = None) -> list:
        """Get most active users by message count in the last N days."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                fclause, fparams = self._member_filter_clause(user_ids, column="m.user_id")
                cutoff = int(datetime.now(timezone.utc).timestamp()) - (days * SECONDS_PER_DAY)
                cursor.execute(f"""
                    SELECT m.user_id, m.username, m.nickname, SUM(ma.message_count) as total_messages
                    FROM message_activity ma
                    JOIN members m ON ma.user_id = m.user_id AND ma.guild_id = m.guild_id
                    WHERE ma.guild_id = ? AND ma.date >= ? AND m.is_active = 1{fclause}
                    GROUP BY m.user_id, m.username, m.nickname
                    ORDER BY total_messages DESC
                    LIMIT ?
                """, (guild_id, cutoff, *fparams, limit))
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

    # ==================== Privacy / Opt-Out Operations ====================

    def add_opted_out_user(self, user_id: int) -> bool:
        """Add a user to the global tracking opt-out list."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO opted_out_users (user_id, opted_out_at)
                    VALUES (?, ?)
                """, (user_id, int(datetime.now(timezone.utc).timestamp())))
                return True
        except Exception as e:
            logger.error(f"Failed to add opted-out user {user_id}: {e}")
            return False

    def remove_opted_out_user(self, user_id: int) -> bool:
        """Remove a user from the global tracking opt-out list."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM opted_out_users WHERE user_id = ?", (user_id,))
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to remove opted-out user {user_id}: {e}")
            return False

    def get_opted_out_user_ids(self) -> set:
        """Get the set of all opted-out user IDs."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM opted_out_users")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to get opted-out users: {e}")
            return set()

    def purge_user_data(self, user_id: int) -> Optional[Dict[str, int]]:
        """
        Delete all tracked data for a user across all guilds.

        Deleting the member rows cascades to role_changes, message_activity,
        and message_activity_hourly via their foreign keys.

        Args:
            user_id: Discord user ID

        Returns:
            Dict with per-table deletion counts, or None on failure.
        """
        counts = {'members': 0, 'role_changes': 0, 'message_activity': 0, 'message_activity_hourly': 0}
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Count records before deletion for reporting
                for table in counts:
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE user_id = ?", (user_id,))
                    counts[table] = cursor.fetchone()[0]

                # Deleting the member rows cascades to the dependent tables
                cursor.execute("DELETE FROM members WHERE user_id = ?", (user_id,))

                # Remove any watches that target this user directly. Watchlists
                # have no FK to members (a watch can target a role), so the
                # cascade above does not reach them — delete them explicitly so
                # /forgetme and opt-out fully sever tracking of this user.
                cursor.execute("""
                    DELETE FROM watchlists WHERE target_type = 'user' AND target_id = ?
                """, (user_id,))

                logger.info(
                    f"Purged user {user_id}: {counts['members']} member records, "
                    f"{counts['role_changes']} role changes, "
                    f"{counts['message_activity']} daily and "
                    f"{counts['message_activity_hourly']} hourly activity records"
                )
                return counts
        except Exception as e:
            logger.error(f"Failed to purge data for user {user_id}: {e}")
            return None

    # ==================== Watchlist Operations ====================

    def add_watch(self, guild_id: int, target_type: str, target_id: int,
                  alert_type: str, threshold_seconds: Optional[int],
                  channel_id: Optional[int], created_by: int) -> Optional[int]:
        """Create or update a watch, returning its per-guild display number (seq).

        Re-adding an identical (guild, target, alert_type) watch updates its
        threshold/channel and resets fire-state (keeping its existing seq), so an
        admin can re-issue the command to change settings. New watches get the
        smallest per-guild number not currently in use, so removed numbers are
        reused and each guild stays a compact 1..N. Returns None on failure.

        Note: internal operations (remove, fire-state) key on the row `id` from
        get_guild_watches — seq is purely the user-facing number.
        """
        now = int(datetime.now(timezone.utc).timestamp())
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Smallest per-guild number not already taken (reuses gaps left by
                # removed watches). Admin-initiated and low-concurrency, so the
                # read-then-insert here needs no extra locking.
                cursor.execute("SELECT seq FROM watchlists WHERE guild_id = ?", (guild_id,))
                used = {r[0] for r in cursor.fetchall() if r[0] is not None}
                seq = 1
                while seq in used:
                    seq += 1
                cursor.execute("""
                    INSERT INTO watchlists
                    (guild_id, seq, target_type, target_id, alert_type, threshold_seconds,
                     channel_id, state, fired_targets, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'armed', NULL, ?, ?)
                    ON CONFLICT(guild_id, target_type, target_id, alert_type) DO UPDATE SET
                        threshold_seconds = excluded.threshold_seconds,
                        channel_id = excluded.channel_id,
                        state = 'armed',
                        fired_targets = NULL
                """, (guild_id, seq, target_type, target_id, alert_type,
                      threshold_seconds, channel_id, created_by, now))
                # Re-read to return the true seq (the INSERT's seq is discarded on
                # the conflict/update path, which keeps the existing one).
                cursor.execute("""
                    SELECT seq FROM watchlists
                    WHERE guild_id = ? AND target_type = ? AND target_id = ? AND alert_type = ?
                """, (guild_id, target_type, target_id, alert_type))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to add watch in guild {guild_id}: {e}")
            return None

    def remove_watch(self, guild_id: int, watch_id: int) -> bool:
        """Delete a watch by id, scoped to its guild. False if it didn't exist."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM watchlists WHERE guild_id = ? AND id = ?",
                    (guild_id, watch_id)
                )
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to remove watch {watch_id} in guild {guild_id}: {e}")
            return False

    def get_guild_watches(self, guild_id: int) -> List[Dict[str, Any]]:
        """All watches configured in a guild, ordered by display number (for /watch list)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM watchlists WHERE guild_id = ? ORDER BY seq",
                    (guild_id,)
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get watches for guild {guild_id}: {e}")
            return []

    def count_guild_watches(self, guild_id: int) -> int:
        """Number of watches in a guild (used to enforce the per-guild cap)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM watchlists WHERE guild_id = ?", (guild_id,)
                )
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Failed to count watches for guild {guild_id}: {e}")
            return 0

    def get_watch_guild_ids(self) -> set:
        """Set of guild_ids that have at least one watch (in-memory hot-path index)."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT guild_id FROM watchlists")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to get watch guild ids: {e}")
            return set()

    def get_offline_watches(self) -> List[Dict[str, Any]]:
        """All 'offline_for' watches across guilds, for the periodic sweep."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM watchlists WHERE alert_type = 'offline_for'"
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get offline watches: {e}")
            return []

    def get_watches_for_member(self, guild_id: int, user_id: int,
                               role_ids: List[int], alert_type: str) -> List[Dict[str, Any]]:
        """Watches in a guild that match a member — by user target or any of
        the member's role targets — for the given alert_type.

        Used on presence-online to find both the online_return watches to fire
        and the offline_for watches to re-arm.
        """
        try:
            role_clause = ""
            params: list = [guild_id, alert_type, user_id]
            if role_ids:
                placeholders = ",".join("?" * len(role_ids))
                role_clause = f" OR (target_type = 'role' AND target_id IN ({placeholders}))"
                params.extend(role_ids)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT * FROM watchlists
                    WHERE guild_id = ? AND alert_type = ?
                    AND ((target_type = 'user' AND target_id = ?){role_clause})
                """, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get watches for member {user_id} in guild {guild_id}: {e}")
            return []

    def get_members_last_seen(self, guild_id: int, user_ids: List[int]) -> Dict[int, Optional[int]]:
        """Batch-read last_seen for a set of members (role offline_for sweep)."""
        if not user_ids:
            return {}
        try:
            placeholders = ",".join("?" * len(user_ids))
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT user_id, last_seen FROM members
                    WHERE guild_id = ? AND user_id IN ({placeholders})
                """, (guild_id, *user_ids))
                return {row['user_id']: row['last_seen'] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to batch-read last_seen for guild {guild_id}: {e}")
            return {}

    def update_watch_fire_state(self, watch_id: int, *, state: Optional[str] = None,
                                fired_targets: Optional[str] = None) -> bool:
        """Update whichever fire-state columns are provided for one watch."""
        sets, params = [], []
        if state is not None:
            sets.append("state = ?"); params.append(state)
        if fired_targets is not None:
            sets.append("fired_targets = ?"); params.append(fired_targets)
        if not sets:
            return False
        params.append(watch_id)
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE watchlists SET {', '.join(sets)} WHERE id = ?", params
                )
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to update fire-state for watch {watch_id}: {e}")
            return False

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

    def vacuum_database(self) -> bool:
        """
        Run VACUUM on the database to reclaim space and optimize performance.
        This should be called after significant deletions (e.g., removing stale guilds).

        VACUUM requires an exclusive lock and can take time on large databases.
        It's recommended to run this during low-activity periods (e.g., startup).

        Returns:
            bool: True if VACUUM completed successfully, False otherwise
        """
        try:
            logger.info("Starting database VACUUM operation...")
            start_time = datetime.now(timezone.utc)

            # Get a connection from pool
            # VACUUM cannot run in a transaction, so we handle this specially
            conn = self._get_connection_from_pool()
            try:
                # Get database size before VACUUM
                cursor = conn.cursor()
                cursor.execute("PRAGMA page_count")
                page_count_before = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                size_before_mb = (page_count_before * page_size) / (1024 * 1024)

                # Run VACUUM (this cannot be in a transaction)
                conn.isolation_level = None  # Autocommit mode required for VACUUM
                cursor.execute("VACUUM")
                conn.isolation_level = ""  # Restore default

                # Get database size after VACUUM
                cursor.execute("PRAGMA page_count")
                page_count_after = cursor.fetchone()[0]
                size_after_mb = (page_count_after * page_size) / (1024 * 1024)

                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                reclaimed_mb = size_before_mb - size_after_mb

                logger.info(
                    f"VACUUM completed in {elapsed:.2f}s. "
                    f"Database size: {size_before_mb:.2f}MB -> {size_after_mb:.2f}MB "
                    f"(reclaimed {reclaimed_mb:.2f}MB)"
                )
                return True

            finally:
                self._return_connection_to_pool(conn)

        except Exception as e:
            logger.error(f"VACUUM operation failed: {e}", exc_info=True)
            return False

    def get_bot_statistics(self) -> dict:
        """
        Get global bot statistics across all guilds.

        Every query here is an unindexed full scan (each index on members and
        the activity tables is guild_id-leading, so a cross-guild aggregate
        cannot use them), so the result is cached and may be up to
        BOT_STATS_CACHE_TTL seconds stale.

        Returns:
            dict: 'total_guilds', 'total_users' (active member rows, counted
                  once per guild membership), 'last_24h' (rolling 24 hours)
                  and 'last_7d' (7 calendar days including today, which is
                  still in progress)
        """
        now = int(datetime.now(timezone.utc).timestamp())

        with self._bot_stats_lock:
            if self._bot_stats_cache and now < self._bot_stats_expires:
                return dict(self._bot_stats_cache)

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Total guilds
                cursor.execute("SELECT COUNT(DISTINCT guild_id) FROM guilds")
                total_guilds = cursor.fetchone()[0]

                # Total active member records (not distinct - counts each guild membership)
                cursor.execute("SELECT COUNT(*) FROM members WHERE is_active = 1")
                total_users = cursor.fetchone()[0]

                # Rolling 24h comes from the hourly table; message_activity
                # only has start-of-day granularity, too coarse for this.
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity_hourly
                    WHERE timestamp >= ?
                """, (now - SECONDS_PER_DAY,))
                last_24h = cursor.fetchone()[0]

                # 7 day buckets: today plus the 6 preceding days.
                today = datetime.now(timezone.utc)
                today_start = int(datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp())
                cursor.execute("""
                    SELECT COALESCE(SUM(message_count), 0)
                    FROM message_activity
                    WHERE date >= ?
                """, (today_start - (6 * SECONDS_PER_DAY),))
                last_7d = cursor.fetchone()[0]

                stats = {
                    'total_guilds': total_guilds,
                    'total_users': total_users,
                    'last_24h': last_24h,
                    'last_7d': last_7d
                }

                with self._bot_stats_lock:
                    self._bot_stats_cache = stats
                    self._bot_stats_expires = now + BOT_STATS_CACHE_TTL

                return dict(stats)
        except Exception as e:
            logger.error(f"Failed to get bot statistics: {e}")
            return {
                'total_guilds': 0,
                'total_users': 0,
                'last_24h': 0,
                'last_7d': 0
            }

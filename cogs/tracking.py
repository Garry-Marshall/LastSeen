"""Event tracking cog for monitoring user activity."""

import discord
from discord.ext import commands, tasks
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple
from collections import defaultdict

from database import DatabaseManager
from bot.utils import get_member_roles, create_embed

logger = logging.getLogger(__name__)

# Time constants (in seconds)
FLUSH_INTERVAL = 30  # How often to flush activity buffers
CLEANUP_INTERVAL_HOURS = 24  # How often to run data cleanup
REPORT_CHECK_INTERVAL_HOURS = 1  # How often to check for scheduled reports


class TrackingCog(commands.Cog):
    """Cog for tracking user joins, leaves, updates, and presence changes."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        """Initialize the tracking cog.
        
        Args:
            bot: Discord bot instance
            db: Database manager instance
            config: Bot configuration object
        """
        """
        Initialize tracking cog.

        Args:
            bot: Discord bot instance
            db: Database manager
            config: Bot configuration
        """
        self.bot = bot
        self.db = db
        self.config = config
        
        # Batch processing buffers for message activity
        # Key: (guild_id, user_id, date), Value: count
        self.daily_activity_buffer: Dict[Tuple[int, int, int], int] = defaultdict(int)
        # Key: (guild_id, user_id, timestamp, hour), Value: count
        self.hourly_activity_buffer: Dict[Tuple[int, int, int, int], int] = defaultdict(int)
        
        # Failed writes buffer for retry
        self.failed_daily_writes: Dict[Tuple[int, int, int], int] = defaultdict(int)
        self.failed_hourly_writes: Dict[Tuple[int, int, int, int], int] = defaultdict(int)
        
        # Buffer size limits to prevent memory issues
        self.MAX_BUFFER_SIZE = 10000  # Max entries before forcing flush
        
        # Start background tasks
        self.flush_activity_buffer.start()
        self.cleanup_old_data.start()
        self.check_scheduled_reports.start()
        self.backup_database.start()

    async def _initialize_member_positions(self, guild: discord.Guild) -> bool:
        """
        Initialize member positions based on join order.
        Fetches all members sorted by join date and assigns position numbers.
        
        Args:
            guild: The guild to initialize positions for
            
        Returns:
            bool: True if successful, False otherwise
        """
        guild_id = guild.id
        
        try:
            # Check if already initialized
            if self.db.guild_positions_initialized(guild_id):
                logger.info(f"Member positions already initialized for guild {guild.name}")
                return True
            
            logger.info(f"Starting member position initialization for guild {guild.name}...")
            
            # Fetch all members sorted by join date
            logger.info(f"Fetching members for {guild.name}...")
            all_members = [m async for m in guild.fetch_members(limit=None)]
            # Filter out bots - only track human members
            members = [m for m in all_members if not m.bot]
            logger.info(f"Fetched {len(all_members)} total members ({len(members)} human members), sorting by join date...")
            members_sorted = sorted(members, key=lambda m: m.joined_at or datetime.now(timezone.utc))
            
            logger.info(f"Fetched {len(members_sorted)} human members, assigning positions...")
            
            # Batch update in chunks (1000 at a time)
            chunk_size = 1000
            updated_count = 0
            for i in range(0, len(members_sorted), chunk_size):
                chunk = members_sorted[i:i+chunk_size]
                for idx, member in enumerate(chunk, start=i+1):
                    # Ensure member exists in database first
                    self._ensure_member_exists(member)
                    # Then set their join position
                    if self.db.set_member_join_position(guild_id, member.id, idx):
                        updated_count += 1
                
                logger.info(f"Processed {min(i+chunk_size, len(members_sorted))}/{len(members_sorted)} members ({updated_count} updated)")
                await asyncio.sleep(0.1)  # Small delay between chunks to avoid overwhelming the bot
            
            # Mark as initialized
            if self.db.mark_positions_initialized(guild_id):
                logger.info(f"Successfully initialized positions for {updated_count}/{len(members_sorted)} members in {guild.name}")
                return True
            else:
                logger.error(f"Failed to mark guild {guild_id} as initialized after updating positions")
                return False
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching members for guild {guild.name} - positions not initialized")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize member positions for guild {guild.name}: {e}", exc_info=True)
            return False

    def _should_track_member(self, member: discord.Member) -> bool:
        """Check if member should be tracked based on guild settings.
        
        Applies role-based and channel-based tracking filters if configured.
        
        Args:
            member: Discord member to check
            
        Returns:
            bool: True if member should be tracked, False otherwise
        """
        """
        Check if a member should be tracked based on guild configuration.

        Args:
            member: Discord member to check

        Returns:
            bool: True if member should be tracked, False otherwise
        """
        import json

        # Get guild config
        guild_config = self.db.get_guild_config(member.guild.id)
        if not guild_config:
            return True  # If no config, track all members

        # Check if track_only_roles is configured
        track_only_roles_json = guild_config.get('track_only_roles')
        if not track_only_roles_json:
            return True  # If no role filter, track all members

        try:
            track_only_roles = json.loads(track_only_roles_json)
            if not track_only_roles:
                return True  # Empty list means track all

            # Check if member has any of the required roles
            member_role_names = [role.name for role in member.roles]
            for required_role in track_only_roles:
                if required_role in member_role_names:
                    return True

            return False  # Member doesn't have any required roles
        except:
            return True  # If error parsing, default to tracking

    def _ensure_member_exists(self, member: discord.Member) -> bool:
        """Ensure member exists in database, creating or updating record if needed.
        
        Args:
            member: Discord member to ensure exists
            
        Returns:
            bool: True if member record exists or was created successfully
        """
        """
        Ensure a member exists in the database, adding them if not.

        Args:
            member: Discord member to check/add

        Returns:
            bool: True if member was added, False if already existed
        """
        # Check if member should be tracked based on role filter
        if not self._should_track_member(member):
            return False

        guild_id = member.guild.id
        user_id = member.id

        if not self.db.member_exists(guild_id, user_id):
            roles = get_member_roles(member)
            join_date = int(member.joined_at.timestamp()) if member.joined_at else int(datetime.now(timezone.utc).timestamp())
            nickname = member.display_name if member.display_name != str(member) else None

            self.db.add_member(
                guild_id=guild_id,
                user_id=user_id,
                username=str(member),
                nickname=nickname,
                join_date=join_date,
                roles=roles
            )
            return True
        return False

    @commands.Cog.listener()
    async def on_ready(self):
        """Called when the bot is ready."""
        logger.info(f"Bot logged in as {self.bot.user}")
        logger.info(f"Connected to {len(self.bot.guilds)} guilds")

        # Ensure all guilds exist in database (handles restart edge case)
        # This is necessary if the database was cleared or the bot restarted
        missing_guilds = 0
        for guild in self.bot.guilds:
            if not self.db.get_guild_config(guild.id):
                self.db.add_guild(guild.id, guild.name)
                missing_guilds += 1
                logger.info(f"Guild '{guild.name}' (ID: {guild.id}) was added to database (on_ready sync)")
        
        if missing_guilds > 0:
            logger.warning(f"Added {missing_guilds} missing guild(s) to database during on_ready sync")

        # Initialize member positions for guilds that haven't been initialized yet
        # This handles the case where the bot restarts or joins existing guilds
        logger.info("Checking for guilds needing member position initialization...")
        for guild in self.bot.guilds:
            if not self.db.guild_positions_initialized(guild.id):
                logger.info(f"Guild {guild.name} needs position initialization, scheduling background task...")
                asyncio.create_task(self._initialize_member_positions(guild))

        # Set bot presence - "Playing Watching you"
        await self.bot.change_presence(
            activity=discord.Game(name='Watching you')
        )

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        """
        Called when the bot joins a guild.
        Adds guild to database and enumerates all members.

        Args:
            guild: The guild that was joined
        """
        logger.info(f"Joined guild: {guild.name} (ID: {guild.id})")

        # Add guild to database
        self.db.add_guild(
            guild_id=guild.id,
            guild_name=guild.name,
            inactive_days=self.config.default_inactive_days
        )

        # Enumerate and add all members
        member_count = 0
        online_count = 0
        for member in guild.members:
            if member.bot:
                continue  # Skip bots

            try:
                roles = get_member_roles(member)
                join_date = int(member.joined_at.timestamp()) if member.joined_at else 0

                # Use display_name if it differs from username, otherwise None
                nickname = member.display_name if member.display_name != str(member) else None

                self.db.add_member(
                    guild_id=guild.id,
                    user_id=member.id,
                    username=str(member),
                    nickname=nickname,
                    join_date=join_date,
                    roles=roles
                )
                
                # Check initial status and set last_seen accordingly
                if member.status != discord.Status.offline:
                    # Member is currently online - set last_seen to 0
                    self.db.update_last_seen(guild.id, member.id, 0)
                    online_count += 1
                # If offline, leave as NULL (never seen online yet)
                
                member_count += 1
            except Exception as e:
                logger.error(f"Failed to add member {member.id} in guild {guild.id}: {e}")

        logger.info(f"Added {member_count} members from guild {guild.name} ({online_count} online, {member_count - online_count} offline/never tracked)")

        # Initialize member positions (for join order tracking)
        # Run as background task to avoid blocking the bot
        asyncio.create_task(self._initialize_member_positions(guild))

    @commands.Cog.listener()
    async def on_guild_remove(self, guild: discord.Guild):
        """
        Called when the bot leaves a guild or the guild is deleted.
        Removes all guild-related data from the database.
        
        Args:
            guild: The guild that was removed
        """
        logger.info(f"Bot left guild: {guild.name} ({guild.id}). Cleaning up database...")
        
        success = self.db.remove_guild_data(guild.id)
        
        if success:
            logger.info(f"Successfully wiped data for guild {guild.id}")
        else:
            logger.error(f"Failed to wipe data for guild {guild.id} during on_guild_remove")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """
        Called when a member joins a guild.

        Args:
            member: The member who joined
        """
        if member.bot:
            return

        logger.info(f"Member joined: {member} in guild {member.guild.name}")

        guild_id = member.guild.id
        user_id = member.id
        roles = get_member_roles(member)
        join_date = int(member.joined_at.timestamp()) if member.joined_at else int(datetime.now(timezone.utc).timestamp())

        # Check if member already exists (rejoining)
        if self.db.member_exists(guild_id, user_id):
            logger.info(f"Member {member} is rejoining guild {member.guild.name}")
            # Update existing record
            nickname = member.display_name if member.display_name != str(member) else None
            self.db.set_member_active(guild_id, user_id)
            self.db.update_member_username(guild_id, user_id, str(member))
            self.db.update_member_nickname(guild_id, user_id, nickname)
            self.db.update_member_roles(guild_id, user_id, roles)
        else:
            # Add new member
            nickname = member.display_name if member.display_name != str(member) else None
            self.db.add_member(
                guild_id=guild_id,
                user_id=user_id,
                username=str(member),
                nickname=nickname,
                join_date=join_date,
                roles=roles
            )
            
            # Check their initial status and set last_seen accordingly
            if member.status != discord.Status.offline:
                # Member joined while online - set last_seen to 0
                self.db.update_last_seen(guild_id, user_id, 0)
                logger.debug(f"New member {member} joined while {member.status} - set last_seen to 0")
            # If offline, leave as NULL (never seen online yet)
            
            # If positions are already initialized, assign position to this new member
            if self.db.guild_positions_initialized(guild_id):
                # Get total member count (approximate position for new member)
                total_members = len(self.db.get_all_guild_members(guild_id))
                self.db.set_member_join_position(guild_id, user_id, total_members)
                logger.debug(f"Assigned join position {total_members} to new member {member}")

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """
        Called when a member leaves a guild.
        Marks member as inactive and posts notification.

        Args:
            member: The member who left
        """
        if member.bot:
            return

        logger.info(f"Member left: {member} from guild {member.guild.name}")

        guild_id = member.guild.id
        user_id = member.id

        # Get member data from database
        member_data = self.db.get_member(guild_id, user_id)
        if not member_data:
            logger.warning(f"Member {user_id} not found in database for guild {guild_id}")
            return

        # Mark member as inactive
        self.db.set_member_inactive(guild_id, user_id)
        
        # Update last_seen to now (current time when they left)
        current_time = int(datetime.now(timezone.utc).timestamp())
        self.db.update_last_seen(guild_id, user_id, current_time)

        # Get guild config for notification channel
        guild_config = self.db.get_guild_config(guild_id)
        if not guild_config or not guild_config['notification_channel_id']:
            logger.info(f"No notification channel set for guild {guild_id}")
            return

        # Send notification
        channel = self.bot.get_channel(guild_config['notification_channel_id'])
        if not channel:
            logger.warning(f"Notification channel {guild_config['notification_channel_id']} not found")
            return
        
        # Validate channel is in correct guild
        if channel.guild.id != guild_id:
            logger.warning(f"Notification channel {guild_config['notification_channel_id']} is not in guild {guild_id}")
            return

        try:
            embed = create_embed("Member Left", discord.Color.red())
            embed.description = ""
            
            # User Identity
            embed.description += f"🆔 User ID: `{member_data['user_id']}`\n"
            embed.description += f"👤 Username: **{member_data['username']}**\n"
            
            # Nickname
            nickname = member_data['nickname'] if member_data['nickname'] else "Not set"
            embed.description += f"🏷️ Nickname: {nickname}\n"
            embed.description += "\n"
            
            # Roles and Highest Role
            if member_data['roles']:
                roles_str = ", ".join(member_data['roles'])
                embed.description += f"🎭 Roles: {roles_str}\n"
                
                # Highest role (using top_role from member object if available)
                if member.roles and len(member.roles) > 1:  # > 1 because everyone has @everyone
                    highest_role = member.top_role
                    if highest_role.name != "@everyone":
                        embed.description += f"⭐ Highest Role: {highest_role.mention}\n"
            else:
                embed.description += f"🎭 Roles: None\n"
            
            embed.description += "\n"
            
            # Membership duration
            if member_data['join_date']:
                join_dt = datetime.fromtimestamp(member_data['join_date'], tz=timezone.utc)
                join_str = discord.utils.format_dt(join_dt, 'F')
                embed.description += f"📥 Joined: {join_str}\n"
                
                # Calculate duration
                now = datetime.now(timezone.utc)
                duration = now - join_dt
                days = duration.days
                if days > 0:
                    embed.description += f"👥 Member for: {days} days\n"
            
            # Last seen
            last_seen_dt = datetime.fromtimestamp(current_time, tz=timezone.utc)
            last_seen_str = discord.utils.format_dt(last_seen_dt, 'R')
            embed.description += f"🚪 Left the Guild: {last_seen_str}\n"

            await channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to send leave notification: {e}")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """
        Called when a member's profile is updated.
        Tracks nickname and role changes.

        Args:
            before: Member state before update
            after: Member state after update
        """
        if after.bot:
            return

        guild_id = after.guild.id
        user_id = after.id

        # Ensure member exists in database
        if self._ensure_member_exists(after):
            return  # Member was just added, no need to check for updates

        # Check for display name change (includes server nickname and global display name)
        before_display = before.display_name if before.display_name != str(before) else None
        after_display = after.display_name if after.display_name != str(after) else None

        if before_display != after_display:
            logger.info(f"Display name changed: {before_display} -> {after_display} for {after}")
            # Track nickname in history BEFORE updating the database
            self.db.update_nickname_history(guild_id, user_id, before_display, after_display)
            # Update the nickname in database
            self.db.update_member_nickname(guild_id, user_id, after_display)

        # Check for username change (display name)
        if str(before) != str(after):
            logger.info(f"Username changed: {before} -> {after}")
            self.db.update_member_username(guild_id, user_id, str(after))

        # Check for role changes
        if before.roles != after.roles:
            roles = get_member_roles(after)
            logger.info(f"Roles changed for {after}: {roles}")
            self.db.update_member_roles(guild_id, user_id, roles)
            
            # Track individual role changes for history
            before_role_names = {role.name for role in before.roles if role.name}  # Filter out None
            after_role_names = {role.name for role in after.roles if role.name}  # Filter out None
            
            # Detect added roles
            added_roles = after_role_names - before_role_names
            for role_name in added_roles:
                if role_name and role_name != "@everyone" and role_name.strip():  # Validate role name
                    self.db.record_role_change(guild_id, user_id, role_name, "added")
                    logger.debug(f"Recorded: Role '{role_name}' added to {after}")
            
            # Detect removed roles
            removed_roles = before_role_names - after_role_names
            for role_name in removed_roles:
                if role_name and role_name != "@everyone" and role_name.strip():  # Validate role name
                    self.db.record_role_change(guild_id, user_id, role_name, "removed")
                    logger.debug(f"Recorded: Role '{role_name}' removed from {after}")

    @commands.Cog.listener()
    async def on_user_update(self, before: discord.User, after: discord.User):
        """
        Called when a user's global profile is updated.
        Tracks global username changes across all guilds.

        Args:
            before: User state before update
            after: User state after update
        """
        if after.bot:
            return

        # Check for username change
        if str(before) != str(after):
            logger.info(f"Global username changed: {before} -> {after}")

            # Update username in all guilds where this user is a member
            for guild in self.bot.guilds:
                member = guild.get_member(after.id)
                if member:
                    guild_id = guild.id
                    user_id = after.id

                    # Update username in database
                    if self.db.member_exists(guild_id, user_id):
                        self.db.update_member_username(guild_id, user_id, str(after))
                        logger.debug(f"Updated username for {after} in guild {guild.name}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        Called when a message is sent in any channel the bot can see.
        Tracks daily aggregate message counts per user per guild.
        
        Args:
            message: The message that was sent
        """
        # Skip if message is from a bot
        if message.author.bot:
            return
        
        # Skip if message is a DM (no guild)
        if message.guild is None:
            return
        
        guild_id = message.guild.id
        user_id = message.author.id
        
        try:
            # Ensure member exists in database
            if not self.db.member_exists(guild_id, user_id):
                logger.debug(f"User {message.author} not tracked in guild {message.guild.name}, skipping message activity")
                return
            
            # Get today's date (start of day UTC)
            now = datetime.now(timezone.utc)
            today_start = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
            
            # Add to daily activity buffer instead of writing immediately
            self.daily_activity_buffer[(guild_id, user_id, today_start)] += 1
            
            # Also buffer hourly activity for heatmap
            hour = now.hour
            hour_start = int(datetime(now.year, now.month, now.day, now.hour, tzinfo=timezone.utc).timestamp())
            self.hourly_activity_buffer[(guild_id, user_id, hour_start, hour)] += 1
            
            # Check if buffers are getting too large and force flush if needed
            total_buffer_size = len(self.daily_activity_buffer) + len(self.hourly_activity_buffer)
            if total_buffer_size >= self.MAX_BUFFER_SIZE:
                logger.warning(f"Buffer size limit reached ({total_buffer_size} entries), forcing flush")
                # Trigger immediate flush by calling the task method directly
                await self.flush_activity_buffer()
            
            logger.debug(f"Buffered message from {message.author} in {message.guild.name}")
        
        except Exception as e:
            logger.error(f"Error tracking message activity: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_presence_update(self, before: discord.Member, after: discord.Member):
        """
        Called when a member's presence changes.
        Updates last_seen timestamp when user goes offline.

        Args:
            before: Member state before presence update
            after: Member state after presence update
        """
        if after.bot:
            return

        guild_id = after.guild.id
        user_id = after.id

        # Ensure member exists in database
        self._ensure_member_exists(after)

        # Track status changes
        if before.status != after.status:
            # Log status change to console
            print(f"{after} ({after.guild}) changed status from {before.status} to {after.status}") # not included in log files
            #logger.info(f"{after} ({after.guild}) changed status from {before.status} to {after.status}") # included in log files

            if after.status == discord.Status.offline:
                # User went offline - record timestamp
                timestamp = int(datetime.now(timezone.utc).timestamp())
                self.db.update_last_seen(guild_id, user_id, timestamp)
                logger.debug(f"User {after} went offline in {after.guild.name}")
            else:
                # User came online - set last_seen to 0 (currently active)
                self.db.update_last_seen(guild_id, user_id, 0)
                logger.debug(f"User {after} is now {after.status} in {after.guild.name}")

    @tasks.loop(seconds=30)
    async def flush_activity_buffer(self):
        """
        Background task that flushes activity buffers to database every 30 seconds.
        This reduces I/O overhead by batching multiple message events into single writes.
        """
        try:
            # Get buffer sizes
            # First, re-add any previously failed writes to the buffer (retry mechanism)
            if self.failed_daily_writes:
                for key, count in self.failed_daily_writes.items():
                    self.daily_activity_buffer[key] += count
                logger.info(f"Re-added {len(self.failed_daily_writes)} failed daily entries for retry")
                self.failed_daily_writes.clear()
                
            if self.failed_hourly_writes:
                for key, count in self.failed_hourly_writes.items():
                    self.hourly_activity_buffer[key] += count
                logger.info(f"Re-added {len(self.failed_hourly_writes)} failed hourly entries for retry")
                self.failed_hourly_writes.clear()
            
            daily_count = len(self.daily_activity_buffer)
            hourly_count = len(self.hourly_activity_buffer)
            
            if daily_count == 0 and hourly_count == 0:
                return  # Nothing to flush
            
            logger.info(f"Flushing activity buffers: {daily_count} daily entries, {hourly_count} hourly entries")
            
            # Copy and clear buffers (atomic swap to avoid missing messages during flush)
            daily_to_flush = dict(self.daily_activity_buffer)
            hourly_to_flush = dict(self.hourly_activity_buffer)
            self.daily_activity_buffer.clear()
            self.hourly_activity_buffer.clear()
            
            # Flush daily activity and track failures
            success_count = 0
            for (guild_id, user_id, date), count in daily_to_flush.items():
                # Pass the count directly instead of calling N times
                if self.db.increment_message_activity(guild_id, user_id, date, count):
                    success_count += 1
                else:
                    # Store failed write for retry on next flush
                    self.failed_daily_writes[(guild_id, user_id, date)] = count
            
            if self.failed_daily_writes:
                logger.warning(f"Failed to flush {len(self.failed_daily_writes)}/{daily_count} daily entries, will retry")
            logger.debug(f"Flushed {success_count}/{daily_count} daily activity entries")
            
            # Flush hourly activity and track failures
            success_count = 0
            for (guild_id, user_id, timestamp, hour), count in hourly_to_flush.items():
                # Pass the count directly instead of calling N times
                if self.db.increment_message_activity_hourly(guild_id, user_id, timestamp, hour, count):
                    success_count += 1
                else:
                    # Store failed write for retry on next flush
                    self.failed_hourly_writes[(guild_id, user_id, timestamp, hour)] = count
            
            if self.failed_hourly_writes:
                logger.warning(f"Failed to flush {len(self.failed_hourly_writes)}/{hourly_count} hourly entries, will retry")
            logger.debug(f"Flushed {success_count}/{hourly_count} hourly activity entries")
            
        except Exception as e:
            logger.error(f"Error flushing activity buffers: {e}", exc_info=True)

    @flush_activity_buffer.before_loop
    async def before_flush_activity_buffer(self):
        """Wait for bot to be ready before starting the flush loop."""
        await self.bot.wait_until_ready()

    @tasks.loop(hours=CLEANUP_INTERVAL_HOURS)
    async def cleanup_old_data(self):
        """
        Background task that cleans up old message activity data daily.
        Runs once every 24 hours based on each guild's retention settings.
        """
        try:
            logger.info("Starting daily data retention cleanup...")
            result = self.db.cleanup_all_guilds_message_activity()
            
            if result['daily_deleted'] > 0 or result['hourly_deleted'] > 0:
                logger.info(
                    f"Data retention cleanup completed: "
                    f"{result['daily_deleted']} daily records, "
                    f"{result['hourly_deleted']} hourly records deleted "
                    f"across {result['guilds_processed']} guilds"
                )
            else:
                logger.debug(f"Data retention cleanup: no old records to delete ({result['guilds_processed']} guilds checked)")
                
        except Exception as e:
            logger.error(f"Error during data retention cleanup: {e}", exc_info=True)

    @cleanup_old_data.before_loop
    async def before_cleanup_old_data(self):
        """Wait for bot to be ready before starting the cleanup loop."""
        await self.bot.wait_until_ready()

    @tasks.loop(hours=REPORT_CHECK_INTERVAL_HOURS)
    async def check_scheduled_reports(self):
        """
        Background task that checks and sends scheduled reports every hour.
        Checks all guilds with reports enabled and sends reports when due.
        """
        try:
            from bot.reports import send_scheduled_report
            import json
            
            logger.debug("Checking scheduled reports...")
            guilds_with_reports = self.db.get_guilds_with_reports_enabled()
            
            if not guilds_with_reports:
                return
            
            now = datetime.now(timezone.utc)
            current_weekday = now.weekday()  # 0=Monday, 6=Sunday
            current_day_of_month = now.day
            current_timestamp = int(now.timestamp())
            
            for guild_config in guilds_with_reports:
                try:
                    guild_id = int(guild_config['guild_id'])
                    guild = self.bot.get_guild(guild_id)
                    
                    if not guild:
                        logger.warning(f"Guild {guild_id} not found, skipping report")
                        continue
                    
                    channel_id = int(guild_config['report_channel_id'])
                    frequency = guild_config['report_frequency']
                    try:
                        report_types = json.loads(guild_config['report_types']) if guild_config['report_types'] else []
                    except (json.JSONDecodeError, TypeError):
                        logger.error(f"Failed to parse report_types JSON for guild {guild_id}")
                        report_types = []
                    
                    if not report_types:
                        continue
                    
                    # Check if weekly report is due
                    if frequency in ['weekly', 'both']:
                        day_weekly = guild_config.get('report_day_weekly', 0)
                        last_weekly = guild_config.get('last_weekly_report', 0)
                        
                        # Check if it's the right day and report hasn't been sent today
                        # Convert last report timestamp to date to prevent duplicates on same day
                        should_send_weekly = False
                        if current_weekday == day_weekly:
                            if last_weekly == 0:
                                # Never sent before
                                should_send_weekly = True
                            else:
                                # Check if last report was sent on a different date (not today)
                                last_report_date = datetime.fromtimestamp(last_weekly, tz=timezone.utc).date()
                                current_date = now.date()
                                should_send_weekly = (current_date != last_report_date)
                        
                        if should_send_weekly:
                            logger.info(f"Sending weekly report for guild {guild.name}")
                            success = await send_scheduled_report(guild, channel_id, self.db, report_types, 7)
                            if success:
                                self.db.update_last_report_time(guild_id, 'weekly')
                    
                    # Check if monthly report is due
                    if frequency in ['monthly', 'both']:
                        day_monthly = guild_config.get('report_day_monthly', 1)
                        last_monthly = guild_config.get('last_monthly_report', 0)
                        
                        # Check if it's the right day and report hasn't been sent today
                        # Convert last report timestamp to date to prevent duplicates on same day
                        should_send_monthly = False
                        if current_day_of_month == day_monthly:
                            if last_monthly == 0:
                                # Never sent before
                                should_send_monthly = True
                            else:
                                # Check if last report was sent on a different date (not today)
                                last_report_date = datetime.fromtimestamp(last_monthly, tz=timezone.utc).date()
                                current_date = now.date()
                                should_send_monthly = (current_date != last_report_date)
                        
                        if should_send_monthly:
                            logger.info(f"Sending monthly report for guild {guild.name}")
                            success = await send_scheduled_report(guild, channel_id, self.db, report_types, 30)
                            if success:
                                self.db.update_last_report_time(guild_id, 'monthly')
                
                except Exception as e:
                    logger.error(f"Error processing scheduled report for guild: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error in check_scheduled_reports task: {e}", exc_info=True)

    @check_scheduled_reports.before_loop
    async def before_check_scheduled_reports(self):
        """Wait for bot to be ready before starting the reports loop."""
        await self.bot.wait_until_ready()

    @tasks.loop(hours=1)  # Check every hour, will use config to determine actual interval
    async def backup_database(self):
        """
        Background task that creates database backups at configured intervals.
        Manages backup retention by deleting old backups.
        """
        try:
            # Check if it's time to backup based on configured interval
            backup_interval_hours = self.config.backup_interval_hours
            
            # Get the last backup time (use a simple file check or track in memory)
            # For simplicity, we'll backup at the configured interval
            # Store last backup time as an attribute
            if not hasattr(self, '_last_backup_time'):
                self._last_backup_time = 0
            
            current_time = datetime.now(timezone.utc).timestamp()
            time_since_last_backup = (current_time - self._last_backup_time) / 3600  # hours
            
            if time_since_last_backup >= backup_interval_hours:
                logger.info(f"Starting database backup (interval: {backup_interval_hours}h)...")
                
                # Create backup
                backup_file = self.db.create_backup(str(self.config.backup_folder))
                
                if backup_file:
                    logger.info(f"Database backup completed: {backup_file}")
                    
                    # Cleanup old backups
                    retention_count = self.config.backup_retention_count
                    deleted = self.db.cleanup_old_backups(str(self.config.backup_folder), retention_count)
                    
                    if deleted > 0:
                        logger.info(f"Deleted {deleted} old backup(s), keeping {retention_count} most recent")
                    
                    # Update last backup time
                    self._last_backup_time = current_time
                else:
                    logger.error("Database backup failed")
                    
        except Exception as e:
            logger.error(f"Error during database backup: {e}", exc_info=True)

    @backup_database.before_loop
    async def before_backup_database(self):
        """Wait for bot to be ready before starting the backup loop."""
        await self.bot.wait_until_ready()

    def cog_unload(self):
        """
        Called when the cog is unloaded.
        Ensures buffered data is flushed before shutdown.
        """
        logger.info("TrackingCog unloading, flushing remaining buffers...")
        self.flush_activity_buffer.cancel()
        self.cleanup_old_data.cancel()
        self.check_scheduled_reports.cancel()
        self.backup_database.cancel()
        
        # Schedule async flush task to run in the event loop
        # This avoids blocking the shutdown process
        async def async_flush():
            try:
                # Use asyncio.to_thread to run DB operations in thread pool
                await asyncio.to_thread(self._flush_buffers_sync)
                logger.info("Successfully flushed all buffers on unload")
            except Exception as e:
                logger.error(f"Error flushing buffers on unload: {e}", exc_info=True)
        
        # Schedule the async flush
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(async_flush())
            else:
                # Fallback to sync if loop not running
                self._flush_buffers_sync()
        except Exception as e:
            logger.error(f"Error scheduling buffer flush: {e}", exc_info=True)
            # Last resort: try synchronous flush
            try:
                self._flush_buffers_sync()
            except Exception as sync_e:
                logger.error(f"Failed to flush buffers synchronously: {sync_e}", exc_info=True)
    
    def _flush_buffers_sync(self):
        """Synchronous version of buffer flush for thread pool execution.
        
        This method is called via asyncio.to_thread() to avoid blocking
        the event loop during shutdown. Flushes all pending activity data
        to the database.
        """
        """Synchronous helper to flush buffers. Called from thread pool."""
        for (guild_id, user_id, date), count in self.daily_activity_buffer.items():
            self.db.increment_message_activity(guild_id, user_id, date, count)
        
        for (guild_id, user_id, timestamp, hour), count in self.hourly_activity_buffer.items():
            self.db.increment_message_activity_hourly(guild_id, user_id, timestamp, hour, count)


async def setup(bot: commands.Bot):
    """
    Setup function for loading the cog.

    Args:
        bot: Discord bot instance
    """
    db = bot.db  # Database manager attached to bot
    config = bot.config  # Config attached to bot
    await bot.add_cog(TrackingCog(bot, db, config))
    logger.info("TrackingCog loaded")

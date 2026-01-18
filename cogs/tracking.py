"""Event tracking cog for monitoring user activity."""

import discord
from discord.ext import commands
import logging
from datetime import datetime, timezone
from typing import Optional

from database import DatabaseManager
from bot.utils import get_member_roles, create_embed

logger = logging.getLogger(__name__)


class TrackingCog(commands.Cog):
    """Cog for tracking user joins, leaves, updates, and presence changes."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
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

    def _should_track_member(self, member: discord.Member) -> bool:
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
                member_count += 1
            except Exception as e:
                logger.error(f"Failed to add member {member.id} in guild {guild.id}: {e}")

        logger.info(f"Added {member_count} members from guild {guild.name}")

    @commands.Cog.listener()
    async def on_guild_remove(self, guild: discord.Guild):
        """
        Called when the bot leaves a guild.

        Args:
            guild: The guild that was left
        """
        logger.info(f"Left guild: {guild.name} (ID: {guild.id})")
        # Note: We don't delete guild data - keep historical records
        # Guild admins can request data deletion if needed

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

        try:
            embed = create_embed("Member Left", discord.Color.red())
            embed.add_field(name="Username", value=member_data['username'], inline=False)
            embed.add_field(
                name="Nickname",
                value=member_data['nickname'] if member_data['nickname'] else "Not set",
                inline=False
            )

            if member_data['roles']:
                roles_str = ", ".join(member_data['roles']) if member_data['roles'] else "None"
                embed.add_field(name="Roles", value=roles_str, inline=False)

            if member_data['join_date']:
                join_dt = datetime.fromtimestamp(member_data['join_date'])
                embed.add_field(
                    name="Joined",
                    value=discord.utils.format_dt(join_dt, 'F'),
                    inline=False
                )

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
            print(f"{after} changed status from {before.status} to {after.status}")

            if after.status == discord.Status.offline:
                # User went offline - record timestamp
                timestamp = int(datetime.now(timezone.utc).timestamp())
                self.db.update_last_seen(guild_id, user_id, timestamp)
                logger.debug(f"User {after} went offline in {after.guild.name}")
            else:
                # User came online - set last_seen to 0 (currently active)
                self.db.update_last_seen(guild_id, user_id, 0)
                logger.debug(f"User {after} is now {after.status} in {after.guild.name}")


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

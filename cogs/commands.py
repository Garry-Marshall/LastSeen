"""User commands cog for querying member information."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import json
import re
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta

from database import DatabaseManager
from bot.utils import (
    parse_user_mention,
    create_embed,
    create_error_embed,
    format_timestamp,
    chunk_list,
    can_use_bot_commands,
    is_channel_allowed,
    has_bot_admin_role
)

logger = logging.getLogger(__name__)


class PaginationView(discord.ui.View):
    """Interactive pagination view for navigating through multiple pages."""

    def __init__(self, embeds: list[discord.Embed], timeout: int = 180):
        """
        Initialize pagination view.

        Args:
            embeds: List of embeds to paginate through
            timeout: Timeout in seconds (default 3 minutes)
        """
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.current_page = 0
        self.max_pages = len(embeds)

        # Disable buttons if only one page
        if self.max_pages == 1:
            self.first_page_button.disabled = True
            self.prev_button.disabled = True
            self.next_button.disabled = True
            self.last_page_button.disabled = True

    @discord.ui.button(label="⏮️", style=discord.ButtonStyle.secondary)
    async def first_page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to first page."""
        self.current_page = 0
        await self._update_message(interaction)

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        self.current_page = max(0, self.current_page - 1)
        await self._update_message(interaction)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        self.current_page = min(self.max_pages - 1, self.current_page + 1)
        await self._update_message(interaction)

    @discord.ui.button(label="⏭️", style=discord.ButtonStyle.secondary)
    async def last_page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to last page."""
        self.current_page = self.max_pages - 1
        await self._update_message(interaction)

    async def _update_message(self, interaction: discord.Interaction):
        """Update the message with current page and button states."""
        # Update button states
        self.first_page_button.disabled = (self.current_page == 0)
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page == self.max_pages - 1)
        self.last_page_button.disabled = (self.current_page == self.max_pages - 1)

        await interaction.response.edit_message(
            embed=self.embeds[self.current_page],
            view=self
        )


class CommandsCog(commands.Cog):
    """Cog for user-facing commands."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        """
        Initialize commands cog.

        Args:
            bot: Discord bot instance
            db: Database manager
            config: Bot configuration
        """
        self.bot = bot
        self.db = db
        self.config = config

    async def _check_permissions(self, interaction: discord.Interaction) -> tuple[bool, discord.Embed | None, bool]:
        """
        Check if user has permission to use commands in this channel.

        Args:
            interaction: Discord interaction

        Returns:
            Tuple of (can_proceed, error_embed, channels_restricted)
            channels_restricted: True if allowed_channels is configured (means output should not be ephemeral)
        """
        guild_config = self.db.get_guild_config(interaction.guild_id)
        channels_restricted = False

        # Check role permissions
        if guild_config and not can_use_bot_commands(interaction.user, guild_config):
            user_role_name = guild_config.get('user_role_name', 'LastSeen User')
            error = create_error_embed(
                f"You need the '{user_role_name}' role or Administrator permission to use this command."
            )
            return False, error, channels_restricted

        # Check channel permissions and determine if channels are restricted
        if guild_config:
            allowed_channels_json = guild_config.get('allowed_channels')
            channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False
            if not is_channel_allowed(interaction.channel_id, guild_config):
                error = create_error_embed(
                    "Bot commands are not allowed in this channel. Please use an allowed channel."
                )
                return False, error, channels_restricted

        return True, None, channels_restricted

    async def user_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> list[app_commands.Choice[str]]:
        """
        Autocomplete callback for user parameters.
        Suggests member names as user types.
        
        Args:
            interaction: Discord interaction
            current: Current user input
            
        Returns:
            List of autocomplete choices
        """
        if not interaction.guild:
            return []
        
        # Get all members from database for this guild
        members = self.db.get_guild_members(interaction.guild_id, include_left=False)
        
        # Filter based on current input (case-insensitive)
        current_lower = current.lower()
        matches = []
        
        for member in members:
            username = member.get('username', '')
            nickname = member.get('nickname', '')
            
            # Match on username or nickname
            if current_lower in username.lower() or (nickname and current_lower in nickname.lower()):
                # Show nickname if available, otherwise username
                display_name = nickname if nickname else username
                matches.append(app_commands.Choice(name=display_name[:100], value=username[:100]))
                
            # Limit to 25 choices (Discord limit)
            if len(matches) >= 25:
                break
        
        return matches

    @app_commands.command(name="whois", description="👤 Get information about a user")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def whois(self, interaction: discord.Interaction, user: str):
        """
        Display information about a user.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=not channels_restricted
            )
            return

        # Get Discord member object for additional info (online status, boosting, etc.)
        member = interaction.guild.get_member(member_data['user_id'])
        
        # Check if caller is admin - get bot admin role name from guild config
        guild_config = self.db.get_guild_config(guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        is_admin = has_bot_admin_role(interaction.user, bot_admin_role_name)
        
        # Create embed with new formatting
        username = member_data['username'] if member_data['username'] else "Unknown"
        embed = create_embed(f"👤 {username}", discord.Color.blue())
        embed.description = ""

        # ===== USER IDENTITY SECTION =====
        embed.description += f"🆔 User ID: `{member_data['user_id']}`\n"
        
        # Account creation date - admin only
        if is_admin and member and hasattr(member, 'created_at'):
            try:
                account_created = format_timestamp(int(member.created_at.timestamp()), 'F', guild_id, self.db)
                embed.description += f"📅 Account Created: {account_created}\n"
            except (AttributeError, ValueError, OSError):
                pass
        
        # Join info with position
        if member_data['join_date']:
            join_str = format_timestamp(member_data['join_date'], 'F', guild_id, self.db)
            join_position = member_data.get('join_position')
            if join_position:
                embed.description += f"📥 Joined Server: {join_str} (Member #{join_position})\n"
            else:
                embed.description += f"📥 Joined Server: {join_str}\n"
        
        embed.description += "\n"

        # ===== NICKNAME & ROLES SECTION =====
        nickname = member_data['nickname'] if member_data['nickname'] else "Not set"
        embed.description += f"🏷️ Nickname: {nickname}\n"
        
        # Nickname history - admin only (exclude current nickname)
        if is_admin and member_data.get('nickname_history'):
            try:
                history = json.loads(member_data['nickname_history'])
                if history:
                    # Filter out the current nickname to show only previous ones
                    previous_nicknames = [n for n in history if n != nickname]
                    if previous_nicknames:
                        history_str = ", ".join(previous_nicknames)
                        embed.description += f"     Previously known as: {history_str}\n"
            except:
                pass
        
        # Roles
        if member_data['roles']:
            roles_str = ", ".join(member_data['roles'])
            embed.description += f"🎭 Roles: {roles_str}\n"
        else:
            embed.description += f"🎭 Roles: None\n"
        
        # Highest role
        if member and hasattr(member, 'roles') and member.roles and len(member.roles) > 1:  # > 1 because everyone has @everyone
            try:
                highest_role = member.top_role
                if highest_role and highest_role.name != "@everyone":
                    embed.description += f"⭐ Highest Role: {highest_role.mention}\n"
            except (AttributeError, IndexError):
                pass
        
        embed.description += "\n"

        # ===== STATUS SECTION =====
        if member:
            # Online status
            status_emoji = {
                discord.Status.online: "🟢",
                discord.Status.idle: "🟡",
                discord.Status.dnd: "🔴",
                discord.Status.offline: "⚫"
            }.get(member.status, "⚫")
            
            embed.description += f"{status_emoji} Status: {str(member.status).capitalize()}\n"
            
            # Last seen
            if hasattr(member, 'status') and member.status != discord.Status.offline:
                embed.description += f"⏱️ Last Seen: Currently online\n"
            elif member_data['last_seen'] and member_data['last_seen'] != 0:
                embed.description += f"⏱️ Last Seen: {format_timestamp(member_data['last_seen'], 'R', guild_id, self.db)}\n"
            else:
                embed.description += f"⏱️ Last Seen: Not available\n"
        else:
            embed.description += f"⚫ Status: Left server\n"
        
        # Boosting status
        if member and hasattr(member, 'premium_since') and member.premium_since:
            try:
                boost_date = format_timestamp(int(member.premium_since.timestamp()), 'F', guild_id, self.db)
                embed.description += f"💎 Boosting: Yes (since {boost_date})\n"
            except (AttributeError, ValueError, OSError):
                pass
        else:
            embed.description += f"💎 Boosting: No\n"

        embed.description += "\n"

        # ===== MESSAGE ACTIVITY SECTION =====
        activity_stats = self.db.get_message_activity_period(guild_id, member_data['user_id'], days=30)
        if activity_stats and (activity_stats['total'] > 0 or activity_stats['today'] >= 0):
            embed.description += f"📊 Activity:\n"
            embed.description += f"     • Today: {activity_stats['today']:,} messages\n"
            embed.description += f"     • This week: {activity_stats['this_week']:,} messages\n"
            embed.description += f"     • This month: {activity_stats['this_month']:,} messages\n"
            if activity_stats['avg_per_day'] > 0:
                embed.description += f"     • Avg/day (7d): {activity_stats['avg_per_day']} messages\n"

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /whois for '{user}' in guild {interaction.guild.name}")

    async def _lastseen_impl(self, interaction: discord.Interaction, user: str, command_name: str):
        """
        Shared implementation for lastseen/seen commands.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
            command_name: Name of command that called this (for logging)
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=not channels_restricted
            )
            return

        # Create embed
        embed = create_embed("Last Seen Information", discord.Color.green())

        embed.add_field(
            name="Username",
            value=member_data['username'] if member_data['username'] else "Not set",
            inline=False
        )
        embed.add_field(
            name="Nickname",
            value=member_data['nickname'] if member_data['nickname'] else "Not set",
            inline=False
        )

        # Check if user is currently online
        member = interaction.guild.get_member(member_data['user_id'])
        if member and member.status != discord.Status.offline:
            embed.add_field(name="Status", value="Currently online", inline=False)
            embed.add_field(name="Last Seen", value="Right meow! 🐱", inline=False)
        else:
            embed.add_field(name="Status", value="Offline", inline=False)
            if member_data['last_seen'] and member_data['last_seen'] != 0:
                embed.add_field(
                    name="Last Seen",
                    value=format_timestamp(member_data['last_seen'], 'R', guild_id, self.db),
                    inline=False
                )
                embed.add_field(
                    name="Exact Time",
                    value=format_timestamp(member_data['last_seen'], 'F', guild_id, self.db),
                    inline=False
                )
            else:
                embed.add_field(name="Last Seen", value="Not available", inline=False)

        # Add status if inactive
        if member_data['is_active'] == 0:
            embed.add_field(name="Note", value="User has left the server", inline=False)

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /{command_name} for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="lastseen", description="👁️ Check when a user was last seen online")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def lastseen(self, interaction: discord.Interaction, user: str):
        """
        Display when a user was last seen online.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        await self._lastseen_impl(interaction, user, "lastseen")

    @app_commands.command(name="seen", description="👁️ Alias for /lastseen - Check when a user was last seen")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def seen(self, interaction: discord.Interaction, user: str):
        """
        Alias for lastseen command.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        await self._lastseen_impl(interaction, user, "seen")

    @app_commands.command(name="role-history", description="📜 View role change history for a member (Admin only)")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def role_history(self, interaction: discord.Interaction, user: str):
        """
        Display role change history for a member (admin only).

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        # Check if user is admin
        guild_config = self.db.get_guild_config(interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'
        
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(f"You need the '{bot_admin_role_name}' role or Administrator permission to use this command."),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=not channels_restricted
            )
            return

        # Get role history
        role_changes = self.db.get_role_history(guild_id, member_data['user_id'], limit=20)

        if not role_changes:
            embed = create_embed(f"🎭 Role History - {member_data['username']}", discord.Color.blue())
            embed.description = "No role changes recorded for this member."
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Create embed with role history
        username = member_data['username'] if member_data['username'] else "Unknown"
        embed = create_embed(f"🎭 Role History - {username}", discord.Color.blue())
        embed.description = f"**Last 20 role changes:**\n\n"

        for change in role_changes:
            role_name = change.get('role_name', 'Unknown')
            action = change.get('action', 'unknown')
            timestamp = change.get('timestamp', 0)
            
            # Validate action value
            if action not in ("added", "removed"):
                logger.warning(f"Invalid action '{action}' in role_changes for {member_data['user_id']}")
                continue
            
            # Escape markdown in role name (avoid embed formatting issues)
            escaped_role_name = discord.utils.escape_markdown(role_name) if role_name else "Unknown"
            
            # Format action with emoji
            action_emoji = "➕" if action == "added" else "➖"
            action_text = "Added" if action == "added" else "Removed"
            time_str = format_timestamp(timestamp, 'R', guild_id, self.db)
            
            embed.description += f"{action_emoji} {action_text}: **{escaped_role_name}** ({time_str})\n"

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /role-history for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="inactive", description="💤 List inactive members (add days to override server threshold)")
    @app_commands.describe(days="Override default threshold - specify number of days (1-365)")
    @app_commands.checks.cooldown(1, 10.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def inactive(self, interaction: discord.Interaction, days: int = None):
        """
        List all members who have been inactive for longer than the configured threshold.

        Args:
            interaction: Discord interaction
            days: Optional override for inactive days threshold
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        # Validate days input if provided
        if days is not None:
            if not (1 <= days <= 365):
                await interaction.response.send_message(
                    embed=create_error_embed("Please provide a value between 1 and 365 days."),
                    ephemeral=True
                )
                return

        # Get guild config
        guild_id = interaction.guild_id
        guild_config = self.db.get_guild_config(guild_id)
        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed("Guild configuration not found. Please contact an administrator."),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        # Use provided days or fall back to config
        inactive_days = days if days is not None else guild_config['inactive_days']

        # Get inactive members
        inactive_members = self.db.get_inactive_members(guild_id, inactive_days)

        if not inactive_members:
            embed = create_embed("Inactive Members", discord.Color.blue())
            embed.description = f"No members have been inactive for more than {inactive_days} days."
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Create paginated embeds (8 members per page)
        chunks = chunk_list(inactive_members, 8)
        embeds = []

        for i, chunk in enumerate(chunks):
            embed = create_embed(
                f"Inactive Members (>{inactive_days} days) - Page {i + 1}/{len(chunks)}",
                discord.Color.blue()
            )

            for member_data in chunk:
                # Create a field for each member
                username = member_data['username'] if member_data['username'] else "Unknown"
                nickname = member_data['nickname'] if member_data['nickname'] else "Not set"
                last_seen = format_timestamp(member_data['last_seen'], 'R', guild_id, self.db) if member_data['last_seen'] else "Never"

                member_info = f"**Nickname:** {nickname}\n**Last Seen:** {last_seen}"
                embed.add_field(name=username, value=member_info, inline=False)

            embeds.append(embed)

        # Send with pagination view
        view = PaginationView(embeds)
        await interaction.followup.send(embed=embeds[0], view=view, ephemeral=not channels_restricted)

        logger.info(f"User {interaction.user} used /inactive in guild {interaction.guild.name} with threshold {inactive_days}")
        logger.info(f"Found {len(inactive_members)} inactive members (>{inactive_days} days)")

    @app_commands.command(name="chat-history", description="📈 View extended message activity history (365 days)")
    @app_commands.describe(user="Username, nickname, or @mention (leave empty for server-wide stats)")
    @app_commands.autocomplete(user=user_autocomplete)
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    @app_commands.guild_only()
    async def chat_history(self, interaction: discord.Interaction, user: str = None):
        """
        Display extended message activity history.
        Shows user stats if specified, or guild-wide stats if not.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention (optional)
        """
        # Check permissions
        can_proceed, error_embed, channels_restricted = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=not channels_restricted, thinking=True)

        guild_id = interaction.guild_id
        
        # If no user provided, show guild-wide stats
        if user is None:
            stats = self.db.get_guild_message_activity_stats(guild_id, days=365)
            
            if stats['total_365d'] == 0:
                embed = create_embed(f"📊 Server Chat History - {interaction.guild.name}", discord.Color.blue())
                embed.description = "No message activity recorded in the last 365 days."
                await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
                return
            
            # Create guild-wide stats embed
            embed = create_embed(f"📊 Server Chat History - {interaction.guild.name}", discord.Color.blue())
            
            embed.description = "**📈 Long-Term Statistics (365 days)**\n"
            embed.description += f"• Total Messages: **{stats['total_365d']:,}**\n"
            embed.description += f"• Average/Day: **{stats['avg_per_day']:,}**\n"
            
            if stats['busiest_day']:
                busiest_str = format_timestamp(stats['busiest_day']['date'], 'D', guild_id, self.db)
                embed.description += f"• Busiest Day: **{stats['busiest_day']['count']:,}** on {busiest_str}\n"
            
            if stats['quietest_day']:
                quietest_str = format_timestamp(stats['quietest_day']['date'], 'D', guild_id, self.db)
                embed.description += f"• Quietest Day: **{stats['quietest_day']['count']:,}** on {quietest_str}\n"
            
            embed.description += "\n**📊 Activity by Period**\n"
            embed.description += f"• Last 30 days: **{stats['total_30d']:,}** messages\n"
            embed.description += f"• Last 90 days: **{stats['total_90d']:,}** messages\n"
            embed.description += f"• Last 365 days: **{stats['total_365d']:,}** messages\n\n"
            
            embed.description += "**📅 Recent Activity**\n"
            embed.description += f"• Today: **{stats['today']:,}** messages\n"
            embed.description += f"• This week: **{stats['total_7d']:,}** messages\n"
            embed.description += f"• This month: **{stats['total_30d']:,}** messages\n\n"
            
            # Get total member count for comparison
            total_members = len([m for m in interaction.guild.members if not m.bot])
            
            embed.description += "**👥 Member Stats**\n"
            embed.description += f"• Active members (30d): **{stats['active_members_30d']:,}** / {total_members:,}\n"
            if stats['avg_per_member'] > 0:
                embed.description += f"• Messages per active member: **{stats['avg_per_member']:,}**\n"
            
            embed.set_footer(text="Activity data spans the last 365 days • Use /chat-history @user for individual stats")
            
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            logger.info(f"User {interaction.user} used /chat-history (guild-wide) in guild {interaction.guild.name}")
            return

        # User-specific stats (existing functionality)
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=not channels_restricted
            )
            return

        user_id = member_data['user_id']
        username = member_data['username'] if member_data['username'] else "Unknown"

        # Get 365 days of message activity
        activity_trend = self.db.get_message_activity_trend(guild_id, user_id, days=365)

        if not activity_trend:
            embed = create_embed(f"📊 Chat History - {username}", discord.Color.blue())
            embed.description = "No message activity recorded in the last 365 days."
            await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
            return

        # Calculate statistics
        total_messages = sum(record['message_count'] for record in activity_trend)
        avg_per_day = round(total_messages / 365, 1)
        max_day = max(activity_trend, key=lambda r: r['message_count'])
        min_day = min(activity_trend, key=lambda r: r['message_count'])
        max_day_str = format_timestamp(max_day['date'], 'D', guild_id, self.db)
        min_day_str = format_timestamp(min_day['date'], 'D', guild_id, self.db)

        # Get summary statistics
        activity_stats_30 = self.db.get_message_activity_period(guild_id, user_id, days=30)
        activity_stats_90 = self.db.get_message_activity_period(guild_id, user_id, days=90)

        # Create main embed with statistics
        embed = create_embed(f"📊 Chat History - {username}", discord.Color.blue())
        
        embed.description = "**📈 Long-Term Statistics (365 days)**\n"
        embed.description += f"• Total Messages: **{total_messages:,}**\n"
        embed.description += f"• Average/Day: **{avg_per_day}**\n"
        embed.description += f"• Busiest Day: **{max_day['message_count']:,}** on {max_day_str}\n"
        embed.description += f"• Quietest Day: **{min_day['message_count']:,}** on {min_day_str}\n\n"
        
        embed.description += "**📊 Activity by Period**\n"
        embed.description += f"• Last 30 days: **{activity_stats_30['this_month']:,}** messages\n"
        embed.description += f"• Last 90 days: **{activity_stats_90['this_month']:,}** messages\n"
        embed.description += f"• Last 365 days: **{total_messages:,}** messages\n\n"
        
        # Calculate monthly breakdown for last 90 days
        if activity_trend:
            now = datetime.now()
            current_month_count = sum(r['message_count'] for r in activity_trend 
                                     if (now.year == datetime.fromtimestamp(r['date']).year and 
                                         now.month == datetime.fromtimestamp(r['date']).month))
            
            embed.description += "**📅 Recent Activity**\n"
            embed.description += f"• This month: **{current_month_count:,}** messages\n"
            embed.description += f"• This week: **{activity_stats_30['this_week']:,}** messages\n"
            embed.description += f"• Today: **{activity_stats_30['today']:,}** messages\n"

        embed.set_footer(text="Activity data spans the last 365 days. Data before account join is unavailable.")

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /chat-history for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="about", description="ℹ️ About this bot")
    async def about(self, interaction: discord.Interaction):
        embed = create_embed("📊 LastSeen", discord.Color.green())

        embed.description = (
            "**LastSeen** is a Discord bot for monitoring and tracking user activity "
            "across guilds.\n\n"
            "It tracks:\n"
            "• User joins and leaves\n"
            "• Nickname changes\n"
            "• Role updates\n"
            "• Presence and activity status\n\n"
            "Designed for server moderators who want clear insight into member activity "
            "without unnecessary noise."
        )

        embed.add_field(
            name="🔐 Privacy",
            value="This bot does **not** store or read message content. "
                "Only metadata required for activity tracking is recorded.",
            inline=False
        )

        embed.add_field(
            name="🔗 Need Help / Have Suggestions?",
            value="[Join Our Community Server](https://discord.gg/d3N5sd58fh)",
            inline=False
        )

        embed.set_footer(text="Use /help to see available commands")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="search", description="🔍 Search and filter server members")
    @app_commands.guild_only()
    @app_commands.describe(
        roles="Filter by roles (comma-separated, e.g., @Mod,@Admin)",
        status="Filter by presence: online, offline, idle, dnd, or all",
        inactive="Days since last seen - use >30 (more than), <7 (less than), or =14 (exactly)",
        activity="Messages in last 30 days - examples: >100, <10, =50",
        joined="Join date filter - format: >2024-01-01, <2023-06-01, =2025-01-15",
        username="Search username (partial match, case-insensitive)",
        export="Export results as file: csv, txt, or none"
    )
    async def search(
        self,
        interaction: discord.Interaction,
        roles: str = None,
        status: str = None,
        inactive: str = None,
        activity: str = None,
        joined: str = None,
        username: str = None,
        export: str = "none"
    ):
        """Advanced member search with filtering and export."""
        # Check admin permission
        if not has_bot_admin_role(interaction.user, interaction.guild):
            await interaction.response.send_message(
                "❌ This command requires admin permissions.",
                ephemeral=True
            )
            return

        # Validate export parameter
        if export and export.lower() not in ['none', 'csv', 'txt']:
            await interaction.response.send_message(
                f"❌ Invalid export format: '{export}'. Use: csv, txt, or none",
                ephemeral=True
            )
            return

        # Check channel restrictions
        guild_config = self.db.get_guild_config(interaction.guild_id)
        allowed_channels_json = guild_config.get('allowed_channels') if guild_config else None
        channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False
        
        # Check if command is allowed in current channel
        if not is_channel_allowed(interaction.channel_id, guild_config):
            await interaction.response.send_message(
                "❌ Bot commands are not allowed in this channel. Please use an allowed channel.",
                ephemeral=True
            )
            return

        # Defer response since this might take a while
        await interaction.response.defer(ephemeral=not channels_restricted)

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("❌ This command can only be used in a server.", ephemeral=True)
            return
        
        guild_id = guild.id

        try:
            # Parse all filter parameters
            filters = self._parse_search_filters(
                roles=roles,
                status=status,
                inactive=inactive,
                activity=activity,
                joined=joined,
                username=username,
                guild=guild
            )
        except ValueError as e:
            await interaction.followup.send(f"❌ Invalid filter syntax: {e}", ephemeral=True)
            return

        # Get all members from database
        db_members = self.db.get_guild_members(guild_id, include_left=False)

        # Get Discord members from cache (pre-chunked in on_ready)
        discord_members = {m.id: m for m in guild.members}

        # Apply filters
        filtered = []
        cache_misses = 0
        
        for member_data in db_members:
            discord_member = discord_members.get(member_data['user_id'])
            
            if not discord_member:
                cache_misses += 1
                # Member not in cache (left server or cache incomplete)
                if filters.get('status') or filters.get('roles'):
                    # Skip - these filters require Discord data
                    continue
                # Database-only filters still work
                if self._matches_db_filters(member_data, filters):
                    filtered.append(self._create_db_only_result(member_data))
            else:
                # Full Discord data available
                if self._matches_all_filters(member_data, discord_member, filters):
                    filtered.append(self._enrich_member_data(member_data, discord_member))

        # Log cache misses
        if cache_misses > 0:
            logger.warning(f"Search had {cache_misses} cache misses in guild {guild_id}")

        # Check if no results
        if len(filtered) == 0:
            await interaction.followup.send(
                "No members found matching your filters. Try adjusting your criteria.",
                ephemeral=not channels_restricted
            )
            return

        # Apply result limit
        MAX_RESULTS = 1000
        if len(filtered) > MAX_RESULTS:
            await interaction.followup.send(
                f"⚠️ Found {len(filtered)} members. Showing first {MAX_RESULTS}. "
                f"Consider adding more filters to narrow results.",
                ephemeral=not channels_restricted
            )
            filtered = filtered[:MAX_RESULTS]

        # Sort by last_seen (most recent first)
        filtered.sort(key=lambda x: x.get('last_seen_ts', 0), reverse=True)

        # Handle export or display
        if export.lower() in ["csv", "txt"]:
            await self._export_search_results(interaction, filtered, export.lower(), filters, channels_restricted)
        else:
            await self._display_search_results(interaction, filtered, filters, channels_restricted)

    @app_commands.command(name="user-stats", description="📊 View server statistics and analytics")
    @app_commands.guild_only()
    async def user_stats(self, interaction: discord.Interaction):
        """
        Display comprehensive server statistics with interactive dashboard.
        Shows overview with buttons to access detailed reports.
        
        Args:
            interaction: Discord interaction
        """
        # Check if user has permission (admin or user role)
        guild_config = self.db.get_guild_config(interaction.guild_id)
        
        if guild_config and not has_bot_admin_role(interaction.user, guild_config.get('bot_admin_role_name', 'LastSeen Admin')):
            # Not admin, check for user role
            user_role_required = guild_config.get('user_role_required', 0)
            if user_role_required:
                user_role_name = guild_config.get('user_role_name', 'LastSeen User')
                if not discord.utils.get(interaction.user.roles, name=user_role_name):
                    await interaction.response.send_message(
                        f"❌ You need the '{user_role_name}' role or admin permissions to use this command.",
                        ephemeral=True
                    )
                    return
        
        # Check channel restrictions
        allowed_channels_json = guild_config.get('allowed_channels') if guild_config else None
        channels_restricted = bool(allowed_channels_json) if allowed_channels_json else False
        
        # Check if command is allowed in current channel
        if not is_channel_allowed(interaction.channel_id, guild_config):
            await interaction.response.send_message(
                "❌ Bot commands are not allowed in this channel. Please use an allowed channel.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=not channels_restricted)
        
        try:
            # Get overview statistics
            stats = self.db.get_server_snapshot_stats(interaction.guild_id)
            
            if not stats:
                await interaction.followup.send(
                    "❌ Failed to retrieve server statistics. Please try again.",
                    ephemeral=not channels_restricted
                )
                return
            
            # Get previous month stats for comparison
            prev_stats = self.db.get_member_growth_stats(interaction.guild_id, days=60)
            growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0
            
            # Create overview embed
            embed = self._create_stats_overview_embed(stats, growth_rate)
            
            # Create interactive view
            view = UserStatsView(interaction.guild_id, self.db)
            
            await interaction.followup.send(embed=embed, view=view, ephemeral=not channels_restricted)
            logger.info(f"User {interaction.user} viewed user-stats in guild {interaction.guild.name}")
            
        except Exception as e:
            logger.error(f"Failed to display user stats: {e}", exc_info=True)
            await interaction.followup.send(
                f"❌ An error occurred while retrieving statistics: {e}",
                ephemeral=not channels_restricted
            )

    def _create_stats_overview_embed(self, stats: dict, growth_rate: float) -> discord.Embed:
        """Create the main overview embed for user stats."""
        embed = create_embed("📊 User Statistics Overview", discord.Color.blue())
        
        # Format growth indicator
        growth_indicator = "📈" if growth_rate > 0 else "📉" if growth_rate < 0 else "➡️"
        growth_text = f"{growth_indicator} {abs(growth_rate):.1f}% vs last month" if growth_rate != 0 else "No change"
        
        # Member counts section
        active_pct = (stats['active_30d'] / stats['total_members'] * 100) if stats['total_members'] > 0 else 0
        inactive_pct = (stats['inactive_30d'] / stats['total_members'] * 100) if stats['total_members'] > 0 else 0
        
        embed.description = (
            f"**👥 Total Members:** {stats['total_members']:,} ({growth_text})\n"
            f"**✅ Active (30d):** {stats['active_30d']:,} ({active_pct:.1f}%)\n"
            f"**💤 Inactive (30d):** {stats['inactive_30d']:,} ({inactive_pct:.1f}%)\n"
        )
        
        # This month section
        net_indicator = "🔼" if stats['net_growth'] > 0 else "🔽" if stats['net_growth'] < 0 else "➡️"
        embed.add_field(
            name="📅 This Month",
            value=(
                f"• New joins: **{stats['joins_this_month']:,}**\n"
                f"• Members left: **{stats['leaves_this_month']:,}**\n"
                f"• Net growth: **{net_indicator} {stats['net_growth']:,}**"
            ),
            inline=False
        )
        
        # Activity section
        embed.add_field(
            name="💬 Activity (30 days)",
            value=(
                f"• Total messages: **{stats['total_messages_30d']:,}**\n"
                f"• Avg per member: **{stats['avg_messages_per_member']:.1f}**\n"
                f"• Most active: **{stats['most_active_user']}** ({stats['most_active_count']:,} msgs)"
            ),
            inline=False
        )
        
        embed.set_footer(text="Click buttons below to view detailed reports")
        return embed

    def _parse_search_filters(self, roles, status, inactive, activity, joined, username, guild) -> dict:
        """Parse and validate all filter parameters."""
        filters = {}

        # Parse roles
        if roles:
            role_list = []
            
            # Detect if input contains Discord role mentions (<@&123>)
            if '<@&' in roles:
                # Discord mention format: split by spaces
                role_strings = roles.split()
            else:
                # Plain text format: split by commas (supports multi-word role names)
                role_strings = [r.strip() for r in roles.split(',')]
            
            logger.info(f"Role filter input: '{roles}' -> parsed as: {role_strings}")
            
            for role_str in role_strings:
                role_str = role_str.strip()
                if not role_str:
                    continue
                    
                # Handle @mention format <@&123456>
                if role_str.startswith('<@&') and role_str.endswith('>'):
                    try:
                        role_id = int(role_str[3:-1])
                        role = guild.get_role(role_id)
                        if role:
                            role_list.append(role.id)
                            logger.info(f"  Found role by mention: {role.name} (ID: {role.id})")
                    except ValueError:
                        # Invalid role ID format, skip
                        logger.warning(f"  Invalid role mention format: {role_str}")
                        continue
                else:
                    # Search by name (case-insensitive)
                    role = discord.utils.find(lambda r: r.name.lower() == role_str.lower(), guild.roles)
                    if role:
                        role_list.append(role.id)
                        logger.info(f"  Found role by name: '{role_str}' -> {role.name} (ID: {role.id})")
                    else:
                        logger.warning(f"  Role not found: '{role_str}'")
            
            if role_list:
                filters['roles'] = role_list
                logger.info(f"Final role filter: {len(role_list)} role(s) - IDs: {role_list}")
            else:
                # User specified role filter but no valid roles found
                # Set to empty list to force no results (instead of None which means "no filter")
                filters['roles'] = []
                logger.warning("No valid roles found in filter - will return 0 results")

        # Parse status
        if status:
            status_lower = status.lower()
            if status_lower not in ['online', 'offline', 'idle', 'dnd', 'all']:
                raise ValueError("Status must be: online, offline, idle, dnd, or all")
            filters['status'] = status_lower if status_lower != 'all' else None

        # Parse inactive (days)
        if inactive:
            filters['inactive'] = self._parse_filter_value(inactive, 'days')

        # Parse activity (message count)
        if activity:
            filters['activity'] = self._parse_filter_value(activity, 'messages')

        # Parse joined date
        if joined:
            filters['joined'] = self._parse_date_filter(joined)

        # Parse username
        if username:
            filters['username'] = username.lower()

        return filters

    def _parse_filter_value(self, filter_str: str, unit: str) -> dict:
        """Parse comparison filters like >30, <7, =14."""
        match = re.match(r'^([<>=]?)(\d+)$', filter_str.strip())
        if not match:
            raise ValueError(f"Invalid {unit} filter format. Use: >30, <7, or =14")
        
        operator = match.group(1) or '='
        try:
            value = int(match.group(2))
        except ValueError:
            raise ValueError(f"Invalid numeric value in {unit} filter")
        
        # Bounds checking to prevent unreasonable values
        if unit == 'days' and (value < 0 or value > 36500):  # ~100 years
            raise ValueError(f"Days value must be between 0 and 36500 (got {value})")
        elif unit == 'messages' and (value < 0 or value > 10000000):  # 10M messages
            raise ValueError(f"Message count must be between 0 and 10,000,000 (got {value})")
        
        return {'operator': operator, 'value': value}

    def _parse_date_filter(self, date_str: str) -> dict:
        """Parse date filters like >2024-01-01."""
        match = re.match(r'^([<>=]?)(\d{4}-\d{2}-\d{2})$', date_str.strip())
        if not match:
            raise ValueError("Invalid date format. Use: >2024-01-01 or <2023-12-31")
        
        operator = match.group(1) or '='
        try:
            date = datetime.strptime(match.group(2), '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid date: {match.group(2)}")
        
        # Validate date is reasonable (Discord launched in 2015)
        if date.year < 2015:
            raise ValueError("Date must be 2015 or later (Discord launch year)")
        if date.year > 2100:
            raise ValueError("Date must be before year 2100")
        
        timestamp = int(date.replace(tzinfo=timezone.utc).timestamp())
        
        return {'operator': operator, 'value': timestamp}

    def _compare(self, actual: float, filter_spec: dict) -> bool:
        """Compare actual value against filter specification."""
        operator = filter_spec['operator']
        expected = filter_spec['value']
        
        if operator == '>':
            return actual > expected
        elif operator == '<':
            return actual < expected
        else:  # '='
            return actual == expected

    def _matches_db_filters(self, member_data: dict, filters: dict) -> bool:
        """Check if member matches database-only filters (no Discord data needed)."""
        # Username filter (searches both username and nickname/display_name)
        if filters.get('username'):
            username_lower = member_data['username'].lower()
            nickname_lower = (member_data.get('nickname') or member_data.get('display_name') or '').lower()
            search_term = filters['username']
            
            # Check if search term appears in either username or nickname
            if search_term not in username_lower and search_term not in nickname_lower:
                return False

        # Inactive filter
        if filters.get('inactive'):
            last_seen = member_data.get('last_seen')
            if last_seen and last_seen > 0:
                # Member has been seen before, calculate days since
                days_inactive = (datetime.now(timezone.utc).timestamp() - last_seen) / 86400
                if not self._compare(days_inactive, filters['inactive']):
                    return False
            else:
                # No last_seen data or last_seen = 0 (currently online/never tracked)
                # Treat as 0 days inactive
                if not self._compare(0, filters['inactive']):
                    return False

        # Joined filter
        if filters.get('joined'):
            # Database uses 'join_date' column, not 'joined_at'
            joined_at = member_data.get('join_date') or member_data.get('joined_at')
            if joined_at:
                if not self._compare(joined_at, filters['joined']):
                    return False
            else:
                # No join date data - exclude from filter
                logger.debug(f"Member {member_data.get('user_id')} has no join_date, excluding from joined filter")
                return False

        return True

    def _matches_all_filters(self, member_data: dict, discord_member: discord.Member, filters: dict) -> bool:
        """Check if member matches all filters (both DB and Discord data)."""
        # First check database filters
        if not self._matches_db_filters(member_data, filters):
            return False

        # Role filter (Discord data)
        if filters.get('roles') is not None:  # Check for None specifically, not just falsy
            member_role_ids = {r.id for r in discord_member.roles}
            # If filter is empty list, no members match
            if not filters['roles']:
                return False
            # Otherwise check if member has any of the specified roles
            if not any(role_id in member_role_ids for role_id in filters['roles']):
                return False

        # Status filter (Discord data)
        if filters.get('status'):
            if str(discord_member.status) != filters['status']:
                return False

        # Activity filter (requires database query)
        if filters.get('activity'):
            try:
                activity_data = self.db.get_message_activity_period(
                    member_data['guild_id'],
                    member_data['user_id'],
                    days=30
                )
                if not self._compare(activity_data.get('total', 0), filters['activity']):
                    return False
            except Exception as e:
                logger.error(f"Failed to get activity data for user {member_data['user_id']}: {e}")
                # Treat as 0 activity on error
                if not self._compare(0, filters['activity']):
                    return False

        return True

    def _create_db_only_result(self, member_data: dict) -> dict:
        """Create result dict from database data only (no Discord data)."""
        last_seen = member_data.get('last_seen')
        return {
            'username': member_data['username'],
            'display_name': member_data.get('display_name', ''),
            'user_id': member_data['user_id'],
            'status': 'Unknown',
            'last_seen': last_seen,
            'last_seen_ts': last_seen if last_seen is not None else 0,
            'last_seen_str': self._format_relative_time(last_seen),
            'joined_at': member_data.get('join_date'),
            'joined_at_str': self._format_relative_time(member_data.get('join_date')),
            'join_position': member_data.get('join_position', 'N/A'),
            'roles': [],
            'is_tracked': member_data.get('is_tracked', True),
            'activity_30d': 0,
            'activity_7d': 0,
            'activity_today': 0
        }

    def _enrich_member_data(self, member_data: dict, discord_member: discord.Member) -> dict:
        """Enrich database data with Discord member information."""
        last_seen = member_data.get('last_seen')
        
        # If last_seen is NULL, it means never tracked yet - keep as None to show "Never"
        # If user is currently online but DB has an old timestamp, update to 0
        if discord_member.status != discord.Status.offline and last_seen and last_seen > 0:
            # User is online but DB has an old offline timestamp
            # Set to 0 to show "Online now"
            last_seen = 0
        
        # Get activity data with error handling
        try:
            activity_data = self.db.get_message_activity_period(
                member_data['guild_id'],
                member_data['user_id'],
                days=30
            )
        except Exception as e:
            logger.error(f"Failed to get activity data for user {member_data['user_id']}: {e}")
            activity_data = {'total': 0, 'this_week': 0, 'today': 0}
        
        return {
            'username': discord_member.name,
            'display_name': discord_member.display_name,
            'user_id': discord_member.id,
            'status': str(discord_member.status),
            'last_seen': last_seen,
            'last_seen_ts': last_seen if last_seen is not None else 0,
            'last_seen_str': self._format_relative_time(last_seen),
            'joined_at': member_data.get('join_date'),
            'joined_at_str': self._format_relative_time(member_data.get('join_date')),
            'join_position': member_data.get('join_position', 'N/A'),
            'roles': [r.name for r in discord_member.roles if r.name != '@everyone'],
            'is_tracked': member_data.get('is_tracked', True),
            'activity_30d': activity_data.get('total', 0),
            'activity_7d': activity_data.get('this_week', 0),
            'activity_today': activity_data.get('today', 0)
        }

    def _format_relative_time(self, timestamp: int) -> str:
        """Format timestamp as relative time."""
        if timestamp is None:
            return 'Never'
        
        if timestamp == 0:
            return 'Online now'
        
        # Validate timestamp is reasonable (not negative, not too far in future)
        if timestamp < 0:
            return 'Invalid date'
        
        try:
            now = datetime.now(timezone.utc)
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return 'Invalid date'
        delta = now - dt
        
        if delta.days > 365:
            return f"{delta.days // 365}y ago"
        elif delta.days > 30:
            return f"{delta.days // 30}mo ago"
        elif delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds > 3600:
            return f"{delta.seconds // 3600}h ago"
        elif delta.seconds > 60:
            return f"{delta.seconds // 60}m ago"
        else:
            return "Just now"

    async def _display_search_results(self, interaction: discord.Interaction, results: list, filters: dict, channels_restricted: bool = False):
        """Display search results with pagination."""
        # Create SearchResultsView
        view = SearchResultsView(results, filters, per_page=15)
        embed = view.create_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=not channels_restricted)

    async def _export_search_results(self, interaction: discord.Interaction, results: list, format: str, filters: dict, channels_restricted: bool = False):
        """Export search results to file."""
        try:
            if format == "csv":
                file = self._generate_csv(results)
            else:  # txt
                file = self._generate_txt(results, filters)
            
            await interaction.followup.send(
                f"✅ Exported {len(results)} members to {format.upper()}",
                file=file,
                ephemeral=not channels_restricted
            )
        except Exception as e:
            logger.error(f"Failed to generate export: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Failed to generate export: {e}", ephemeral=not channels_restricted)

    def _generate_csv(self, results: list) -> discord.File:
        """Generate CSV export with all member data."""
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'Username', 'Display Name', 'User ID', 'Status',
            'Last Seen', 'Joined At', 'Join Position',
            'Messages (30d)', 'Messages (7d)', 'Messages (Today)',
            'Roles', 'Is Tracked'
        ], quoting=csv.QUOTE_ALL)  # Quote all fields for safety
        
        writer.writeheader()
        for member in results:
            # Sanitize strings to prevent CSV injection
            username = str(member['username']).replace('\n', ' ').replace('\r', '')
            display_name = str(member.get('display_name', '')).replace('\n', ' ').replace('\r', '')
            roles = ', '.join(str(r).replace('\n', ' ').replace('\r', '') for r in member.get('roles', []))
            
            writer.writerow({
                'Username': username,
                'Display Name': display_name,
                'User ID': member['user_id'],
                'Status': member.get('status', 'Unknown'),
                'Last Seen': member['last_seen_str'],
                'Joined At': member['joined_at_str'],
                'Join Position': member.get('join_position', 'N/A'),
                'Messages (30d)': member.get('activity_30d', 0),
                'Messages (7d)': member.get('activity_7d', 0),
                'Messages (Today)': member.get('activity_today', 0),
                'Roles': roles,
                'Is Tracked': 'Yes' if member['is_tracked'] else 'No'
            })
        
        output.seek(0)
        filename = f"member_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return discord.File(fp=StringIO(output.getvalue()), filename=filename)

    def _generate_txt(self, results: list, filters: dict) -> discord.File:
        """Generate readable text export."""
        output = StringIO()
        output.write(f"Member Search Results - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
        output.write(f"Total Members: {len(results)}\n")
        
        # Write filter summary
        filter_text = self._format_filter_summary(filters)
        if filter_text:
            output.write(f"\nFilters Applied:\n{filter_text}\n")
        
        output.write("=" * 80 + "\n\n")
        
        for i, member in enumerate(results, 1):
            output.write(f"{i}. {member['username']}")
            if member['display_name'] and member['display_name'] != member['username']:
                output.write(f" ({member['display_name']})")
            output.write("\n")
            output.write(f"   User ID: {member['user_id']}\n")
            output.write(f"   Status: {member.get('status', 'Unknown')}\n")
            output.write(f"   Last Seen: {member['last_seen_str']}\n")
            output.write(f"   Joined: {member['joined_at_str']}\n")
            output.write(f"   Activity: {member.get('activity_30d', 0)} msgs (30d), "
                        f"{member.get('activity_7d', 0)} msgs (7d), "
                        f"{member.get('activity_today', 0)} today\n")
            if member.get('roles'):
                output.write(f"   Roles: {', '.join(member['roles'])}\n")
            output.write("\n")
        
        output.seek(0)
        filename = f"member_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        return discord.File(fp=output, filename=filename)

    def _format_filter_summary(self, filters: dict) -> str:
        """Format filter dictionary into readable summary."""
        lines = []
        if filters.get('roles'):
            lines.append(f"  • Roles: {len(filters['roles'])} role(s)")
        if filters.get('status'):
            lines.append(f"  • Status: {filters['status']}")
        if filters.get('inactive'):
            lines.append(f"  • Inactive: {filters['inactive']['operator']}{filters['inactive']['value']} days")
        if filters.get('activity'):
            lines.append(f"  • Activity: {filters['activity']['operator']}{filters['activity']['value']} messages")
        if filters.get('joined'):
            try:
                date_str = datetime.fromtimestamp(filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(f"  • Joined: {filters['joined']['operator']}{date_str}")
            except (ValueError, OSError, OverflowError):
                lines.append(f"  • Joined: {filters['joined']['operator']}[Invalid Date]")
        if filters.get('username'):
            lines.append(f"  • Username contains: '{filters['username']}'")
        return '\n'.join(lines) if lines else None


class SearchResultsView(discord.ui.View):
    """Interactive pagination view for search results."""

    def __init__(self, results: list, filters: dict, per_page: int = 15):
        super().__init__(timeout=300)  # 5 minute timeout
        self.results = results if results else []
        self.filters = filters
        self.per_page = max(1, per_page)  # Ensure at least 1 per page
        self.current_page = 0
        self.max_page = max(0, (len(self.results) - 1) // self.per_page) if self.results else 0
        
        # Update button states
        self._update_buttons()

    def _update_buttons(self):
        """Update button enabled/disabled states."""
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page == self.max_page)
        
        # Disable all buttons if only one page
        if self.max_page == 0:
            self.prev_button.disabled = True
            self.next_button.disabled = True

    def create_embed(self) -> discord.Embed:
        """Create embed for current page."""
        # Bounds check to prevent index errors
        if not self.results:
            return discord.Embed(
                title="🔍 Member Search Results",
                description="No results to display",
                color=discord.Color.blue()
            )
        
        # Ensure current_page is within bounds
        self.current_page = max(0, min(self.current_page, self.max_page))
        
        start = self.current_page * self.per_page
        end = min(start + self.per_page, len(self.results))
        page_results = self.results[start:end]
        
        embed = discord.Embed(
            title="🔍 Member Search Results",
            description=f"Found **{len(self.results)}** members",
            color=discord.Color.blue()
        )
        
        # Add filter summary
        filter_text = self._format_filters()
        if filter_text:
            # Discord embed field value limit is 1024 characters
            if len(filter_text) > 1024:
                filter_text = filter_text[:1021] + "..."
            embed.add_field(name="Filters Applied", value=filter_text, inline=False)
        
        # Add results
        result_lines = []
        for i, member in enumerate(page_results, start=start+1):
            status_emoji = {
                'online': '🟢',
                'idle': '🟡',
                'dnd': '🔴',
                'offline': '⚫',
                'Unknown': '⚪'
            }.get(member.get('status', 'Unknown'), '⚪')
            
            # Sanitize member data to prevent display issues
            username = str(member['username'])[:32]  # Discord max username length
            display_name = str(member.get('display_name', ''))[:32] if member.get('display_name') else None
            
            line = f"**{i}.** {status_emoji} {username}"
            if display_name and display_name != username:
                line += f" *({display_name})*"
            line += f"\n   Last seen: {member['last_seen_str']}"
            if member.get('activity_30d', 0) > 0:
                line += f" • {member['activity_30d']} msgs (30d)"
            result_lines.append(line)
        
        # Join results and check field value limit (1024 characters)
        result_text = '\n\n'.join(result_lines)
        if len(result_text) > 1024:
            # If too long, truncate with warning
            result_text = result_text[:1000] + "\n\n... (truncated, use export for full list)"
        
        embed.add_field(name="Members", value=result_text, inline=False)
        
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.max_page + 1} • Use buttons to navigate or export")
        return embed

    def _format_filters(self) -> str:
        """Format filters into readable string."""
        lines = []
        if self.filters.get('roles'):
            lines.append(f"• **Roles:** {len(self.filters['roles'])} role(s)")
        if self.filters.get('status'):
            lines.append(f"• **Status:** {self.filters['status']}")
        if self.filters.get('inactive'):
            lines.append(f"• **Inactive:** {self.filters['inactive']['operator']}{self.filters['inactive']['value']} days")
        if self.filters.get('activity'):
            lines.append(f"• **Activity:** {self.filters['activity']['operator']}{self.filters['activity']['value']} msgs")
        if self.filters.get('joined'):
            try:
                date_str = datetime.fromtimestamp(self.filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                lines.append(f"• **Joined:** {self.filters['joined']['operator']}{date_str}")
            except (ValueError, OSError, OverflowError):
                lines.append(f"• **Joined:** {self.filters['joined']['operator']}[Invalid Date]")
        if self.filters.get('username'):
            lines.append(f"• **Username:** contains '{self.filters['username']}'")
        return '\n'.join(lines) if lines else "No filters applied"

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        self.current_page = max(0, self.current_page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        self.current_page = min(self.max_page, self.current_page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="📄 Export CSV", style=discord.ButtonStyle.green)
    async def export_csv_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export results as CSV."""
        await interaction.response.defer(ephemeral=True)
        try:
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                'Username', 'Display Name', 'User ID', 'Status',
                'Last Seen', 'Joined At', 'Join Position',
                'Messages (30d)', 'Messages (7d)', 'Messages (Today)',
                'Roles', 'Is Tracked'
            ], quoting=csv.QUOTE_ALL)  # Quote all fields for safety
            
            writer.writeheader()
            for member in self.results:
                # Sanitize strings to prevent CSV injection
                username = str(member['username']).replace('\n', ' ').replace('\r', '')
                display_name = str(member.get('display_name', '')).replace('\n', ' ').replace('\r', '')
                roles = ', '.join(str(r).replace('\n', ' ').replace('\r', '') for r in member.get('roles', []))
                
                writer.writerow({
                    'Username': username,
                    'Display Name': display_name,
                    'User ID': member['user_id'],
                    'Status': member.get('status', 'Unknown'),
                    'Last Seen': member['last_seen_str'],
                    'Joined At': member['joined_at_str'],
                    'Join Position': member.get('join_position', 'N/A'),
                    'Messages (30d)': member.get('activity_30d', 0),
                    'Messages (7d)': member.get('activity_7d', 0),
                    'Messages (Today)': member.get('activity_today', 0),
                    'Roles': roles,
                    'Is Tracked': 'Yes' if member['is_tracked'] else 'No'
                })
            
            output.seek(0)
            filename = f"member_search_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            file = discord.File(fp=StringIO(output.getvalue()), filename=filename)
            await interaction.followup.send(
                f"✅ Exported {len(self.results)} members to CSV",
                file=file,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Export failed: {e}", ephemeral=True)

    @discord.ui.button(label="📝 Export TXT", style=discord.ButtonStyle.green)
    async def export_txt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export results as TXT."""
        await interaction.response.defer(ephemeral=True)
        try:
            output = StringIO()
            output.write(f"Member Search Results - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
            output.write(f"Total Members: {len(self.results)}\n")
            
            # Write filter summary
            filter_lines = []
            if self.filters.get('roles'):
                filter_lines.append(f"  • Roles: {len(self.filters['roles'])} role(s)")
            if self.filters.get('status'):
                filter_lines.append(f"  • Status: {self.filters['status']}")
            if self.filters.get('inactive'):
                filter_lines.append(f"  • Inactive: {self.filters['inactive']['operator']}{self.filters['inactive']['value']} days")
            if self.filters.get('activity'):
                filter_lines.append(f"  • Activity: {self.filters['activity']['operator']}{self.filters['activity']['value']} messages")
            if self.filters.get('joined'):
                date_str = datetime.fromtimestamp(self.filters['joined']['value'], tz=timezone.utc).strftime('%Y-%m-%d')
                filter_lines.append(f"  • Joined: {self.filters['joined']['operator']}{date_str}")
            if self.filters.get('username'):
                filter_lines.append(f"  • Username contains: '{self.filters['username']}'")
            
            if filter_lines:
                output.write(f"\nFilters Applied:\n")
                output.write('\n'.join(filter_lines) + '\n')
            
            output.write("=" * 80 + "\n\n")
            
            for i, member in enumerate(self.results, 1):
                output.write(f"{i}. {member['username']}")
                if member['display_name'] and member['display_name'] != member['username']:
                    output.write(f" ({member['display_name']})")
                output.write("\n")
                output.write(f"   User ID: {member['user_id']}\n")
                output.write(f"   Status: {member.get('status', 'Unknown')}\n")
                output.write(f"   Last Seen: {member['last_seen_str']}\n")
                output.write(f"   Joined: {member['joined_at_str']}\n")
                output.write(f"   Activity: {member.get('activity_30d', 0)} msgs (30d), "
                            f"{member.get('activity_7d', 0)} msgs (7d), "
                            f"{member.get('activity_today', 0)} today\n")
                if member.get('roles'):
                    output.write(f"   Roles: {', '.join(member['roles'])}\n")
                output.write("\n")
            
            output.seek(0)
            filename = f"member_search_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
            file = discord.File(fp=output, filename=filename)
            await interaction.followup.send(
                f"✅ Exported {len(self.results)} members to TXT",
                file=file,
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Export failed: {e}", ephemeral=True)


class UserStatsView(discord.ui.View):
    """Interactive view for user statistics dashboard."""

    def __init__(self, guild_id: int, db: DatabaseManager):
        super().__init__(timeout=300)  # 5 minute timeout
        self.guild_id = guild_id
        self.db = db
        self.current_view = 'overview'

    @discord.ui.button(label="📊 Retention Report", style=discord.ButtonStyle.primary, row=0)
    async def retention_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show retention cohort analysis."""
        await interaction.response.defer()
        
        try:
            cohorts = self.db.get_retention_cohorts(self.guild_id)
            embed = self._create_retention_embed(cohorts)
            
            # Create view with back button
            view = discord.ui.View(timeout=300)
            back_button = discord.ui.Button(label="◀️ Back to Overview", style=discord.ButtonStyle.secondary)
            
            async def back_callback(interaction: discord.Interaction):
                await interaction.response.defer()
                stats = self.db.get_server_snapshot_stats(self.guild_id)
                prev_stats = self.db.get_member_growth_stats(self.guild_id, days=60)
                growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0
                
                # Get the parent cog to access _create_stats_overview_embed
                cog = interaction.client.get_cog('CommandsCog')
                overview_embed = cog._create_stats_overview_embed(stats, growth_rate)
                overview_view = UserStatsView(self.guild_id, self.db)
                
                await interaction.edit_original_response(embed=overview_embed, view=overview_view)
            
            back_button.callback = back_callback
            view.add_item(back_button)
            
            await interaction.edit_original_response(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Failed to show retention report: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @discord.ui.button(label="📈 Server Growth", style=discord.ButtonStyle.primary, row=0)
    async def growth_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show server growth trends."""
        await interaction.response.defer()
        
        try:
            # Get growth data for multiple periods
            growth_30d = self.db.get_member_growth_stats(self.guild_id, days=30)
            growth_90d = self.db.get_member_growth_stats(self.guild_id, days=90)
            growth_365d = self.db.get_member_growth_stats(self.guild_id, days=365)
            
            embed = self._create_growth_embed(growth_30d, growth_90d, growth_365d)
            
            # Create view with back button
            view = discord.ui.View(timeout=300)
            back_button = discord.ui.Button(label="◀️ Back to Overview", style=discord.ButtonStyle.secondary)
            
            async def back_callback(interaction: discord.Interaction):
                await interaction.response.defer()
                stats = self.db.get_server_snapshot_stats(self.guild_id)
                prev_stats = self.db.get_member_growth_stats(self.guild_id, days=60)
                growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0
                
                cog = interaction.client.get_cog('CommandsCog')
                overview_embed = cog._create_stats_overview_embed(stats, growth_rate)
                overview_view = UserStatsView(self.guild_id, self.db)
                
                await interaction.edit_original_response(embed=overview_embed, view=overview_view)
            
            back_button.callback = back_callback
            view.add_item(back_button)
            
            await interaction.edit_original_response(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Failed to show growth report: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @discord.ui.button(label="🏆 Leaderboard", style=discord.ButtonStyle.primary, row=0)
    async def leaderboard_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show activity leaderboard."""
        await interaction.response.defer()
        
        try:
            # Show leaderboard view with period selector
            view = LeaderboardView(self.guild_id, self.db)
            embed = await view.create_leaderboard_embed(days=30)
            
            await interaction.edit_original_response(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Failed to show leaderboard: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @discord.ui.button(label="🔥 Activity Heatmap", style=discord.ButtonStyle.primary, row=1)
    async def heatmap_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show activity heatmap."""
        await interaction.response.defer()
        
        try:
            day_activity = self.db.get_activity_by_day(self.guild_id, days=30)
            hour_activity = self.db.get_activity_by_hour(self.guild_id, days=30)
            embed = self._create_heatmap_embed(day_activity, hour_activity)
            
            # Create view with back button
            view = discord.ui.View(timeout=300)
            back_button = discord.ui.Button(label="◀️ Back to Overview", style=discord.ButtonStyle.secondary)
            
            async def back_callback(interaction: discord.Interaction):
                await interaction.response.defer()
                stats = self.db.get_server_snapshot_stats(self.guild_id)
                prev_stats = self.db.get_member_growth_stats(self.guild_id, days=60)
                growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0
                
                cog = interaction.client.get_cog('CommandsCog')
                overview_embed = cog._create_stats_overview_embed(stats, growth_rate)
                overview_view = UserStatsView(self.guild_id, self.db)
                
                await interaction.edit_original_response(embed=overview_embed, view=overview_view)
            
            back_button.callback = back_callback
            view.add_item(back_button)
            
            await interaction.edit_original_response(embed=embed, view=view)
            
        except Exception as e:
            logger.error(f"Failed to show activity heatmap: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

    @discord.ui.button(label="📋 Export CSV", style=discord.ButtonStyle.green, row=1)
    async def export_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Export comprehensive stats to CSV."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            stats = self.db.get_server_snapshot_stats(self.guild_id)
            growth_30d = self.db.get_member_growth_stats(self.guild_id, days=30)
            growth_90d = self.db.get_member_growth_stats(self.guild_id, days=90)
            leaderboard = self.db.get_activity_leaderboard(self.guild_id, days=30, limit=25)
            
            # Generate CSV
            output = StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_ALL)
            
            # Header
            writer.writerow(['Server Statistics Report'])
            writer.writerow(['Generated', datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')])
            writer.writerow([])
            
            # Overview
            writer.writerow(['OVERVIEW'])
            writer.writerow(['Total Members', stats['total_members']])
            writer.writerow(['Active (30d)', stats['active_30d']])
            writer.writerow(['Inactive (30d)', stats['inactive_30d']])
            writer.writerow([])
            
            # Growth
            writer.writerow(['GROWTH (30 DAYS)'])
            writer.writerow(['Joins', growth_30d.get('joins', 0)])
            writer.writerow(['Leaves', growth_30d.get('leaves', 0)])
            writer.writerow(['Net Growth', growth_30d.get('net_growth', 0)])
            writer.writerow(['Growth Rate %', f"{growth_30d.get('growth_rate', 0):.2f}"])
            writer.writerow([])
            
            # Activity
            writer.writerow(['ACTIVITY (30 DAYS)'])
            writer.writerow(['Total Messages', stats['total_messages_30d']])
            writer.writerow(['Avg per Member', f"{stats['avg_messages_per_member']:.1f}"])
            writer.writerow([])
            
            # Leaderboard
            writer.writerow(['TOP 25 MOST ACTIVE MEMBERS'])
            writer.writerow(['Rank', 'Username', 'Display Name', 'Messages (30d)'])
            for i, member in enumerate(leaderboard, 1):
                writer.writerow([
                    i,
                    member['username'],
                    member['display_name'],
                    member['total_messages']
                ])
            
            output.seek(0)
            filename = f"server_stats_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            file = discord.File(fp=StringIO(output.getvalue()), filename=filename)
            
            await interaction.followup.send(
                f"✅ Exported server statistics to CSV",
                file=file,
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Failed to export stats: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Export failed: {e}", ephemeral=True)

    def _create_retention_embed(self, cohorts: dict) -> discord.Embed:
        """Create retention report embed."""
        embed = create_embed("📊 Member Retention Report", discord.Color.purple())
        
        embed.description = "Shows retention rates for members who joined in different time periods."
        
        for period, data in cohorts.items():
            if data['total_joined'] > 0:
                period_name = {
                    '30d': 'Last 30 Days',
                    '60d': '31-60 Days Ago',
                    '90d': '61-90 Days Ago'
                }.get(period, period)
                
                embed.add_field(
                    name=f"📅 {period_name}",
                    value=(
                        f"• Joined: **{data['total_joined']:,}** members\n"
                        f"• Still in server: **{data['still_active']:,}**\n"
                        f"• Active recently: **{data['active_recently']:,}**\n"
                        f"• Retention rate: **{data['retention_rate']:.1f}%**"
                    ),
                    inline=False
                )
        
        if not cohorts or all(c['total_joined'] == 0 for c in cohorts.values()):
            embed.description = "Not enough data to calculate retention rates."
        
        return embed

    def _create_growth_embed(self, growth_30d: dict, growth_90d: dict, growth_365d: dict) -> discord.Embed:
        """Create server growth embed."""
        embed = create_embed("📈 Server Growth Trends", discord.Color.green())
        
        periods = [
            ("Last 30 Days", growth_30d),
            ("Last 90 Days", growth_90d),
            ("Last 365 Days", growth_365d)
        ]
        
        for period_name, data in periods:
            if data:
                growth_indicator = "📈" if data['growth_rate'] > 0 else "📉" if data['growth_rate'] < 0 else "➡️"
                
                embed.add_field(
                    name=f"📅 {period_name}",
                    value=(
                        f"• Joins: **{data['joins']:,}**\n"
                        f"• Leaves: **{data['leaves']:,}**\n"
                        f"• Net: **{data['net_growth']:+,}**\n"
                        f"• Growth rate: **{growth_indicator} {abs(data['growth_rate']):.2f}%**"
                    ),
                    inline=True
                )
        
        return embed

    def _create_heatmap_embed(self, day_activity: dict, hour_activity: dict) -> discord.Embed:
        """Create activity heatmap embed."""
        embed = create_embed("🔥 Activity Heatmap (Last 30 Days)", discord.Color.orange())
        
        if not day_activity or sum(day_activity.values()) == 0:
            embed.description = "Not enough activity data to generate heatmap."
            return embed
        
        # Day of week breakdown
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        max_day_count = max(day_activity.values()) if day_activity.values() else 1
        
        chart_lines = []
        for day in days_order:
            count = day_activity.get(day, 0)
            bar_length = int((count / max_day_count) * 20) if max_day_count > 0 else 0
            bar = "█" * bar_length + "░" * (20 - bar_length)
            # Fixed width for day name (9 chars) and right-aligned count
            chart_lines.append(f"{day:<9} {bar} {count:>6,}")
        
        # Wrap in code block for monospaced alignment
        embed.add_field(
            name="📊 Activity by Day of Week",
            value="```\n" + "\n".join(chart_lines) + "\n```",
            inline=False
        )
        
        # Peak day
        if day_activity:
            peak_day = max(day_activity, key=day_activity.get)
            peak_count = day_activity[peak_day]
            embed.add_field(
                name="🎯 Peak Day",
                value=f"**{peak_day}** with **{peak_count:,}** messages",
                inline=True
            )
        
        # Hour of day breakdown
        if hour_activity and sum(hour_activity.values()) > 0:
            max_hour_count = max(hour_activity.values())
            hour_lines = []
            
            # Group hours into 6-hour blocks for better readability
            time_blocks = [
                ("Night    ", range(0, 6)),
                ("Morning  ", range(6, 12)),
                ("Afternoon", range(12, 18)),
                ("Evening  ", range(18, 24))
            ]
            
            for block_name, hour_range in time_blocks:
                block_total = sum(hour_activity.get(h, 0) for h in hour_range)
                bar_length = int((block_total / (max_hour_count * 6)) * 15) if max_hour_count > 0 else 0
                bar = "█" * bar_length + "░" * (15 - bar_length)
                # Format with fixed width for count column
                hour_lines.append(f"{block_name} {bar} {block_total:>5,}")
            
            # Wrap in code block for monospaced alignment
            embed.add_field(
                name="⏰ Activity by Time of Day",
                value="```\n" + "\n".join(hour_lines) + "\n```",
                inline=False
            )
            
            # Peak hour
            peak_hour = max(hour_activity, key=hour_activity.get)
            peak_hour_count = hour_activity[peak_hour]
            time_label = f"{peak_hour:02d}:00-{(peak_hour+1)%24:02d}:00"
            embed.add_field(
                name="⏰ Peak Hour",
                value=f"**{time_label}** with **{peak_hour_count:,}** messages",
                inline=True
            )
        
        return embed


class LeaderboardView(discord.ui.View):
    """Interactive leaderboard view with period selection."""

    def __init__(self, guild_id: int, db: DatabaseManager):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.db = db
        self.current_period = 30

    async def create_leaderboard_embed(self, days: int) -> discord.Embed:
        """Create leaderboard embed for specified period."""
        self.current_period = days
        
        period_name = {
            7: "Last 7 Days",
            30: "Last 30 Days",
            90: "Last 90 Days",
            0: "All Time"
        }.get(days, f"Last {days} Days")
        
        embed = create_embed(f"🏆 Activity Leaderboard - {period_name}", discord.Color.gold())
        
        leaderboard = self.db.get_activity_leaderboard(self.guild_id, days=days, limit=10)
        
        if not leaderboard:
            embed.description = "No activity data available for this period."
            return embed
        
        lines = []
        medals = ["🥇", "🥈", "🥉"]
        for i, member in enumerate(leaderboard, 1):
            medal = medals[i-1] if i <= 3 else f"**{i}.**"
            lines.append(
                f"{medal} **{member['display_name']}** - {member['total_messages']:,} messages"
            )
        
        embed.description = "\n".join(lines)
        embed.set_footer(text="Select a time period below to view different rankings")
        
        return embed

    @discord.ui.select(
        placeholder="Select time period",
        options=[
            discord.SelectOption(label="Last 7 Days", value="7", emoji="📅"),
            discord.SelectOption(label="Last 30 Days", value="30", emoji="📅", default=True),
            discord.SelectOption(label="Last 90 Days", value="90", emoji="📅"),
            discord.SelectOption(label="All Time", value="0", emoji="🌟")
        ],
        row=0
    )
    async def period_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        """Handle period selection."""
        await interaction.response.defer()
        
        days = int(select.values[0])
        embed = await self.create_leaderboard_embed(days)
        
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="◀️ Back to Overview", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Return to main overview."""
        await interaction.response.defer()
        
        try:
            stats = self.db.get_server_snapshot_stats(self.guild_id)
            prev_stats = self.db.get_member_growth_stats(self.guild_id, days=60)
            growth_rate = prev_stats.get('growth_rate', 0) if prev_stats else 0
            
            cog = interaction.client.get_cog('CommandsCog')
            overview_embed = cog._create_stats_overview_embed(stats, growth_rate)
            overview_view = UserStatsView(self.guild_id, self.db)
            
            await interaction.edit_original_response(embed=overview_embed, view=overview_view)
            
        except Exception as e:
            logger.error(f"Failed to return to overview: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    """
    Setup function for loading the cog.

    Args:
        bot: Discord bot instance
    """
    db = bot.db
    config = bot.config
    await bot.add_cog(CommandsCog(bot, db, config))
    logger.info("CommandsCog loaded")

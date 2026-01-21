"""User commands cog for querying member information."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
import json
from datetime import datetime

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

    @app_commands.command(name="whois", description="Get information about a user")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
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
                account_created = format_timestamp(int(member.created_at.timestamp()), 'F')
                embed.description += f"📅 Account Created: {account_created}\n"
            except (AttributeError, ValueError, OSError):
                pass
        
        # Join info with position
        if member_data['join_date']:
            join_str = format_timestamp(member_data['join_date'], 'F')
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
                embed.description += f"⏱️ Last Seen: {format_timestamp(member_data['last_seen'], 'R')}\n"
            else:
                embed.description += f"⏱️ Last Seen: Not available\n"
        else:
            embed.description += f"⚫ Status: Left server\n"
        
        # Boosting status
        if member and hasattr(member, 'premium_since') and member.premium_since:
            try:
                boost_date = format_timestamp(int(member.premium_since.timestamp()), 'F')
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
                    value=format_timestamp(member_data['last_seen'], 'R'),
                    inline=False
                )
                embed.add_field(
                    name="Exact Time",
                    value=format_timestamp(member_data['last_seen'], 'F'),
                    inline=False
                )
            else:
                embed.add_field(name="Last Seen", value="Not available", inline=False)

        # Add status if inactive
        if member_data['is_active'] == 0:
            embed.add_field(name="Note", value="User has left the server", inline=False)

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /{command_name} for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="lastseen", description="Check when a user was last seen online")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
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

    @app_commands.command(name="seen", description="Alias for /lastseen - Check when a user was last seen")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
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

    @app_commands.command(name="role-history", description="View role change history for a member (Admin only)")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
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
            time_str = format_timestamp(timestamp, 'R')
            
            embed.description += f"{action_emoji} {action_text}: **{escaped_role_name}** ({time_str})\n"

        await interaction.followup.send(embed=embed, ephemeral=not channels_restricted)
        logger.info(f"User {interaction.user} used /role-history for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="inactive", description="List members who have been inactive")
    @app_commands.describe(days="Optional: Override the configured inactive days threshold (1-365)")
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
                last_seen = format_timestamp(member_data['last_seen'], 'R') if member_data['last_seen'] else "Never"

                member_info = f"**Nickname:** {nickname}\n**Last Seen:** {last_seen}"
                embed.add_field(name=username, value=member_info, inline=False)

            embeds.append(embed)

        # Send with pagination view
        view = PaginationView(embeds)
        await interaction.followup.send(embed=embeds[0], view=view, ephemeral=not channels_restricted)

        logger.info(f"User {interaction.user} used /inactive in guild {interaction.guild.name} with threshold {inactive_days}")
        logger.info(f"Found {len(inactive_members)} inactive members (>{inactive_days} days)")

    @app_commands.command(name="chat-history", description="View extended message activity history (365 days)")
    @app_commands.describe(user="Username, nickname, or @mention (leave empty for server-wide stats)")
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
                busiest_str = format_timestamp(stats['busiest_day']['date'], 'D')
                embed.description += f"• Busiest Day: **{stats['busiest_day']['count']:,}** on {busiest_str}\n"
            
            if stats['quietest_day']:
                quietest_str = format_timestamp(stats['quietest_day']['date'], 'D')
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
        max_day_str = format_timestamp(max_day['date'], 'D')
        min_day_str = format_timestamp(min_day['date'], 'D')

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

    @app_commands.command(name="about", description="About this bot")
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

"""User commands cog for querying member information."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime

from database import DatabaseManager
from bot.utils import (
    parse_user_mention,
    create_embed,
    create_error_embed,
    format_timestamp,
    chunk_list,
    can_use_bot_commands,
    is_channel_allowed
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

    async def _check_permissions(self, interaction: discord.Interaction) -> tuple[bool, discord.Embed | None]:
        """
        Check if user has permission to use commands in this channel.

        Args:
            interaction: Discord interaction

        Returns:
            Tuple of (can_proceed, error_embed)
        """
        guild_config = self.db.get_guild_config(interaction.guild_id)

        # Check role permissions
        if guild_config and not can_use_bot_commands(interaction.user, guild_config):
            user_role_name = guild_config.get('user_role_name', 'LastSeen User')
            error = create_error_embed(
                f"You need the '{user_role_name}' role or Administrator permission to use this command."
            )
            return False, error

        # Check channel permissions
        if guild_config and not is_channel_allowed(interaction.channel_id, guild_config):
            error = create_error_embed(
                "Bot commands are not allowed in this channel. Please use an allowed channel."
            )
            return False, error

        return True, None

    @app_commands.command(name="whois", description="Get information about a user")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
    async def whois(self, interaction: discord.Interaction, user: str):
        """
        Display information about a user.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        # Check permissions
        can_proceed, error_embed = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=True
            )
            return

        # Create embed with user information
        embed = create_embed("User Information", discord.Color.blue())

        # Add basic info
        embed.add_field(name="User ID", value=str(member_data['user_id']), inline=False)
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

        # Add roles
        if member_data['roles']:
            roles_str = ", ".join(member_data['roles'])
            embed.add_field(name="Roles", value=roles_str, inline=False)
        else:
            embed.add_field(name="Roles", value="None", inline=False)

        # Add join date
        if member_data['join_date']:
            embed.add_field(
                name="Member Since",
                value=format_timestamp(member_data['join_date'], 'F'),
                inline=False
            )

        # Add last seen
        if member_data['last_seen'] and member_data['last_seen'] != 0:
            embed.add_field(
                name="Last Seen",
                value=format_timestamp(member_data['last_seen'], 'R'),
                inline=False
            )
        else:
            # Check if user is currently online
            member = interaction.guild.get_member(member_data['user_id'])
            if member and member.status != discord.Status.offline:
                embed.add_field(name="Last Seen", value="Currently online", inline=False)
            else:
                embed.add_field(name="Last Seen", value="Not available", inline=False)

        # Add status
        status_value = "Left server" if member_data['is_active'] == 0 else "Active"
        embed.add_field(name="Status", value=status_value, inline=False)

        await interaction.followup.send(embed=embed, ephemeral=True)
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
        can_proceed, error_embed = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild_id = interaction.guild_id
        search_term = parse_user_mention(user)

        # Find member in database
        member_data = self.db.find_member_by_name(guild_id, search_term)

        if not member_data:
            await interaction.followup.send(
                embed=create_error_embed("User not found in the database."),
                ephemeral=True
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

        await interaction.followup.send(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} used /{command_name} for '{user}' in guild {interaction.guild.name}")

    @app_commands.command(name="lastseen", description="Check when a user was last seen online")
    @app_commands.describe(user="Username, nickname, or @mention of the user")
    @app_commands.checks.cooldown(1, 5.0, key=lambda i: i.user.id)
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
    async def seen(self, interaction: discord.Interaction, user: str):
        """
        Alias for lastseen command.

        Args:
            interaction: Discord interaction
            user: Username, nickname, or user mention
        """
        await self._lastseen_impl(interaction, user, "seen")

    @app_commands.command(name="inactive", description="List members who have been inactive")
    @app_commands.checks.cooldown(1, 10.0, key=lambda i: i.user.id)
    async def inactive(self, interaction: discord.Interaction):
        """
        List all members who have been inactive for longer than the configured threshold.

        Args:
            interaction: Discord interaction
        """
        # Check permissions
        can_proceed, error_embed = await self._check_permissions(interaction)
        if not can_proceed:
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
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

        await interaction.response.defer(ephemeral=True, thinking=True)

        inactive_days = guild_config['inactive_days']

        # Get inactive members
        inactive_members = self.db.get_inactive_members(guild_id, inactive_days)

        if not inactive_members:
            embed = create_embed("Inactive Members", discord.Color.blue())
            embed.description = f"No members have been inactive for more than {inactive_days} days."
            await interaction.followup.send(embed=embed, ephemeral=True)
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
        await interaction.followup.send(embed=embeds[0], view=view, ephemeral=True)

        logger.info(f"User {interaction.user} used /inactive in guild {interaction.guild.name}")
        logger.info(f"Found {len(inactive_members)} inactive members (>{inactive_days} days)")

    @app_commands.command(name="about", description="About this bot")
    async def about(self, interaction: discord.Interaction):
        embed = create_embed("📊 LastSeen", discord.Color.green())

        embed.description = (
            "**LastSeen** is a modular Discord bot for monitoring and tracking user activity "
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
            name="🔗 Source Code",
            value="[View on GitHub](https://github.com/Garry-Marshall/LastSeen)",
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

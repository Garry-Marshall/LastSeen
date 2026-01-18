"""Admin commands cog for bot configuration."""

import discord
from discord import app_commands
from discord.ext import commands
import logging
from datetime import datetime, timezone

from database import DatabaseManager
from bot.utils import (
    has_bot_admin_role,
    create_embed,
    create_error_embed,
    create_success_embed,
    get_member_roles
)

logger = logging.getLogger(__name__)


class ConfigView(discord.ui.View):
    """Interactive view for bot configuration."""

    def __init__(self, db: DatabaseManager, guild_id: int, config):
        """
        Initialize config view.

        Args:
            db: Database manager
            guild_id: Discord guild ID
            config: Bot configuration
        """
        super().__init__(timeout=300)  # 5 minute timeout
        self.db = db
        self.guild_id = guild_id
        self.config = config

        # Get guild config to set toggle button color
        guild_config = self.db.get_guild_config(guild_id)
        if guild_config:
            user_role_required = guild_config.get('user_role_required', 0)
            # Set button style based on state: green if enabled, red if disabled
            self.toggle_user_role_required.style = discord.ButtonStyle.success if user_role_required else discord.ButtonStyle.danger

    @discord.ui.button(label="Set Notification Channel", style=discord.ButtonStyle.primary, emoji="📢", row=0)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the notification channel."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for channel input
        modal = ChannelModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Inactive Days", style=discord.ButtonStyle.primary, emoji="📅", row=0)
    async def set_inactive_days(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the inactive days threshold."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for days input
        modal = InactiveDaysModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Update All Members", style=discord.ButtonStyle.success, emoji="🔄", row=0)
    async def update_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to enumerate and update all members."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        added_count = 0
        updated_count = 0

        try:
            # First, ensure guild name is correct
            self.db.update_guild_name(guild.id, guild.name)

            for member in guild.members:
                if member.bot:
                    continue

                roles = get_member_roles(member)
                join_date = int(member.joined_at.timestamp()) if member.joined_at else int(datetime.now(timezone.utc).timestamp())
                nickname = member.display_name if member.display_name != str(member) else None

                if self.db.member_exists(guild.id, member.id):
                    # Update existing member
                    self.db.update_member_username(guild.id, member.id, str(member))
                    self.db.update_member_nickname(guild.id, member.id, nickname)
                    self.db.update_member_roles(guild.id, member.id, roles)
                    updated_count += 1
                else:
                    # Add new member
                    self.db.add_member(
                        guild_id=guild.id,
                        user_id=member.id,
                        username=str(member),
                        nickname=nickname,
                        join_date=join_date,
                        roles=roles
                    )
                    added_count += 1

            embed = create_success_embed(
                f"Successfully updated member database!\n\n"
                f"**Added:** {added_count} new members\n"
                f"**Updated:** {updated_count} existing members"
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"Updated members in guild {guild.name}: {added_count} added, {updated_count} updated")

        except Exception as e:
            logger.error(f"Failed to update members: {e}")
            await interaction.followup.send(
                embed=create_error_embed(f"Failed to update members: {str(e)}"),
                ephemeral=True
            )

    @discord.ui.button(label="Set Bot Admin Role", style=discord.ButtonStyle.primary, emoji="👑", row=1)
    async def set_bot_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the bot admin role name."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for bot admin role input
        modal = BotAdminRoleModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set User Role", style=discord.ButtonStyle.primary, emoji="👤", row=1)
    async def set_user_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the user role name."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for user role input
        modal = UserRoleModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Toggle User Role Required", emoji="🔐", row=1)
    async def toggle_user_role_required(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to toggle user role requirement."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Get current config
        guild_config = self.db.get_guild_config(self.guild_id)
        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed("Guild configuration not found."),
                ephemeral=True
            )
            return

        # Toggle the setting
        current_value = guild_config.get('user_role_required', 0)
        new_value = not current_value

        # Update database
        if self.db.set_user_role_required(self.guild_id, new_value, interaction.guild.name):
            status = "**enabled**" if new_value else "**disabled**"
            message = (
                f"User role requirement has been {status}.\n\n"
                f"{'Only users with the configured user role can now use bot commands.' if new_value else 'All users can now use bot commands.'}"
            )
            await interaction.response.send_message(
                embed=create_success_embed(message),
                ephemeral=True
            )
            logger.info(f"User role requirement toggled to {new_value} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update user role requirement."),
                ephemeral=True
            )

    @discord.ui.button(label="Set Track Only Roles", style=discord.ButtonStyle.primary, emoji="🎯", row=2)
    async def set_track_only_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set which roles to track (optional)."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for track only roles input
        modal = TrackOnlyRolesModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Allowed Channels", style=discord.ButtonStyle.primary, emoji="📝", row=2)
    async def set_allowed_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set which channels can use commands (optional)."""
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(self.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed("You don't have permission to use this command."),
                ephemeral=True
            )
            return

        # Create modal for allowed channels input
        modal = AllowedChannelsModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Config", style=discord.ButtonStyle.secondary, emoji="⚙️", row=3)
    async def view_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to view current configuration."""
        guild_config = self.db.get_guild_config(self.guild_id)

        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed("Guild configuration not found."),
                ephemeral=True
            )
            return

        embed = create_embed("Current Configuration", discord.Color.blue())

        # Notification channel
        channel_id = guild_config['notification_channel_id']
        if channel_id:
            channel = interaction.guild.get_channel(channel_id)
            channel_str = channel.mention if channel else f"Unknown ({channel_id})"
        else:
            channel_str = "Not set"
        embed.add_field(name="Notification Channel", value=channel_str, inline=False)

        # Inactive days
        embed.add_field(
            name="Inactive Days Threshold",
            value=f"{guild_config['inactive_days']} days",
            inline=False
        )

        # Bot admin role
        bot_admin_role = guild_config.get('bot_admin_role_name', 'Bot Admin')
        embed.add_field(name="Bot Admin Role", value=bot_admin_role, inline=False)

        # User role required
        user_role_required = guild_config.get('user_role_required', 0)
        user_role_required_str = "Yes" if user_role_required else "No"
        embed.add_field(name="User Role Required", value=user_role_required_str, inline=False)

        # User role name
        user_role_name = guild_config.get('user_role_name', 'Bot User')
        embed.add_field(name="User Role Name", value=user_role_name, inline=False)

        # Track only roles
        import json
        track_only_roles = guild_config.get('track_only_roles')
        if track_only_roles:
            try:
                roles_list = json.loads(track_only_roles)
                track_only_str = ", ".join(roles_list) if roles_list else "All roles"
            except:
                track_only_str = "All roles"
        else:
            track_only_str = "All roles"
        embed.add_field(name="Track Only Roles", value=track_only_str, inline=False)

        # Allowed channels
        allowed_channels = guild_config.get('allowed_channels')
        if allowed_channels:
            try:
                channels_list = json.loads(allowed_channels)
                if channels_list:
                    channel_mentions = []
                    for ch_id in channels_list:
                        channel = interaction.guild.get_channel(ch_id)
                        if channel:
                            channel_mentions.append(channel.mention)
                    allowed_channels_str = ", ".join(channel_mentions) if channel_mentions else "All channels"
                else:
                    allowed_channels_str = "All channels"
            except:
                allowed_channels_str = "All channels"
        else:
            allowed_channels_str = "All channels"
        embed.add_field(name="Allowed Channels", value=allowed_channels_str, inline=False)

        # Member count
        member_count = len(self.db.get_all_guild_members(self.guild_id))
        embed.add_field(name="Tracked Members", value=str(member_count), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


class ChannelModal(discord.ui.Modal, title="Set Notification Channel"):
    """Modal for setting the notification channel."""

    channel_input = discord.ui.TextInput(
        label="Channel Name, ID, or Mention",
        placeholder="e.g., general, #general, or 1234567890",
        required=True,
        max_length=100
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        channel_str = self.channel_input.value.strip()
        channel = None

        # Try different parsing methods
        # 1. Check if it's a mention like <#1234567890>
        if channel_str.startswith('<#') and channel_str.endswith('>'):
            try:
                channel_id = int(channel_str[2:-1])
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                pass

        # 2. Check if it's a numeric ID
        if not channel:
            try:
                channel_id = int(channel_str)
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                pass

        # 3. Try to find channel by name (remove # if present)
        if not channel:
            search_name = channel_str.lstrip('#').lower()
            channel = discord.utils.get(
                interaction.guild.text_channels,
                name=search_name
            )

        # If still not found, return error
        if not channel:
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"Channel '{channel_str}' not found. Please use channel name (e.g., 'general'), "
                    f"mention (e.g., '#general'), or ID."
                ),
                ephemeral=True
            )
            return

        # Validate channel type - must be a text channel
        if not isinstance(channel, discord.TextChannel):
            channel_type = type(channel).__name__
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"Invalid channel type. '{channel.name}' is a {channel_type}, "
                    f"but notification channels must be text channels."
                ),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_notification_channel(self.guild_id, channel.id, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"Notification channel set to {channel.mention}"),
                ephemeral=True
            )
            logger.info(f"Notification channel set to {channel.name} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update notification channel."),
                ephemeral=True
            )


class InactiveDaysModal(discord.ui.Modal, title="Set Inactive Days Threshold"):
    """Modal for setting the inactive days threshold."""

    days_input = discord.ui.TextInput(
        label="Number of Days",
        placeholder="Enter number of days (e.g., 10)",
        required=True,
        max_length=3
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        days_str = self.days_input.value.strip()

        try:
            days = int(days_str)
            if days < 1 or days > 365:
                raise ValueError("Days must be between 1 and 365")
        except ValueError as e:
            error_msg = str(e) if "between" in str(e) else "Please enter a valid number between 1 and 365."
            await interaction.response.send_message(
                embed=create_error_embed(error_msg),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_inactive_days(self.guild_id, days, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"Inactive days threshold set to {days} days"),
                ephemeral=True
            )
            logger.info(f"Inactive days threshold set to {days} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update inactive days threshold."),
                ephemeral=True
            )


class BotAdminRoleModal(discord.ui.Modal, title="Set Bot Admin Role"):
    """Modal for setting the bot admin role name."""

    role_input = discord.ui.TextInput(
        label="Bot Admin Role Name",
        placeholder="e.g., LastSeen Admin, Moderator, Admin",
        required=True,
        max_length=100
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        role_name = self.role_input.value.strip()

        # Validate role name
        if not role_name:
            await interaction.response.send_message(
                embed=create_error_embed("Role name cannot be empty."),
                ephemeral=True
            )
            return

        if len(role_name) > 100:
            await interaction.response.send_message(
                embed=create_error_embed("Role name is too long (maximum 100 characters)."),
                ephemeral=True
            )
            return

        # Check if role exists in guild (warning, not error)
        role = discord.utils.get(interaction.guild.roles, name=role_name)
        if not role:
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"⚠️ Warning: Role '{role_name}' does not exist in this server.\n\n"
                    f"The setting has been saved, but you should create this role for it to work properly."
                ),
                ephemeral=True
            )
            # Still update the database
            self.db.set_bot_admin_role(self.guild_id, role_name, interaction.guild.name)
            logger.info(f"Bot admin role set to '{role_name}' in guild {interaction.guild.name} (role doesn't exist yet)")
            return

        # Update database
        if self.db.set_bot_admin_role(self.guild_id, role_name, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"Bot admin role set to **{role_name}**"),
                ephemeral=True
            )
            logger.info(f"Bot admin role set to '{role_name}' in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update bot admin role."),
                ephemeral=True
            )


class UserRoleModal(discord.ui.Modal, title="Set User Role"):
    """Modal for setting the user role name."""

    role_input = discord.ui.TextInput(
        label="User Role Name",
        placeholder="e.g., LastSeen User, Member, Verified",
        required=True,
        max_length=100
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        role_name = self.role_input.value.strip()

        # Validate role name
        if not role_name:
            await interaction.response.send_message(
                embed=create_error_embed("Role name cannot be empty."),
                ephemeral=True
            )
            return

        if len(role_name) > 100:
            await interaction.response.send_message(
                embed=create_error_embed("Role name is too long (maximum 100 characters)."),
                ephemeral=True
            )
            return

        # Check if role exists in guild (warning, not error)
        role = discord.utils.get(interaction.guild.roles, name=role_name)
        if not role:
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"⚠️ Warning: Role '{role_name}' does not exist in this server.\n\n"
                    f"The setting has been saved, but you should create this role for it to work properly."
                ),
                ephemeral=True
            )
            # Still update the database
            self.db.set_user_role_name(self.guild_id, role_name, interaction.guild.name)
            logger.info(f"User role set to '{role_name}' in guild {interaction.guild.name} (role doesn't exist yet)")
            return

        # Update database
        if self.db.set_user_role_name(self.guild_id, role_name, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(f"User role set to **{role_name}**"),
                ephemeral=True
            )
            logger.info(f"User role set to '{role_name}' in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update user role."),
                ephemeral=True
            )


class TrackOnlyRolesModal(discord.ui.Modal, title="Set Track Only Roles"):
    """Modal for setting which roles to track (optional)."""

    roles_input = discord.ui.TextInput(
        label="Role Names (comma-separated)",
        placeholder="e.g., Member, Verified, VIP (leave empty for all roles)",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        roles_str = self.roles_input.value.strip()

        # If empty, track all roles
        if not roles_str:
            if self.db.set_track_only_roles(self.guild_id, [], interaction.guild.name):
                await interaction.response.send_message(
                    embed=create_success_embed("Now tracking all roles (no filter applied)"),
                    ephemeral=True
                )
                logger.info(f"Track only roles cleared in guild {interaction.guild.name}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed("Failed to update track only roles."),
                    ephemeral=True
                )
            return

        # Parse comma-separated role names
        role_names = [r.strip() for r in roles_str.split(',') if r.strip()]

        if not role_names:
            await interaction.response.send_message(
                embed=create_error_embed("No valid role names provided."),
                ephemeral=True
            )
            return

        # Verify roles exist (warning only)
        missing_roles = []
        for role_name in role_names:
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if not role:
                missing_roles.append(role_name)

        # Update database
        if self.db.set_track_only_roles(self.guild_id, role_names, interaction.guild.name):
            message = f"Now tracking only members with these roles: **{', '.join(role_names)}**"
            if missing_roles:
                message += f"\n\n⚠️ Warning: These roles don't exist yet: {', '.join(missing_roles)}"

            await interaction.response.send_message(
                embed=create_success_embed(message),
                ephemeral=True
            )
            logger.info(f"Track only roles set to {role_names} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update track only roles."),
                ephemeral=True
            )


class AllowedChannelsModal(discord.ui.Modal, title="Set Allowed Channels"):
    """Modal for setting which channels can use commands (optional)."""

    channels_input = discord.ui.TextInput(
        label="Channel Names or IDs (comma-separated)",
        placeholder="e.g., general, bot-commands, 123456 (leave empty for all)",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500
    )

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        super().__init__()
        self.db = db
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        channels_str = self.channels_input.value.strip()

        # If empty, allow all channels
        if not channels_str:
            if self.db.set_allowed_channels(self.guild_id, [], interaction.guild.name):
                await interaction.response.send_message(
                    embed=create_success_embed("Bot commands can now be used in all channels"),
                    ephemeral=True
                )
                logger.info(f"Allowed channels cleared in guild {interaction.guild.name}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed("Failed to update allowed channels."),
                    ephemeral=True
                )
            return

        # Parse comma-separated channel names/IDs
        channel_identifiers = [c.strip() for c in channels_str.split(',') if c.strip()]

        if not channel_identifiers:
            await interaction.response.send_message(
                embed=create_error_embed("No valid channel identifiers provided."),
                ephemeral=True
            )
            return

        # Resolve channels
        channel_ids = []
        missing_channels = []

        for identifier in channel_identifiers:
            channel = None

            # Try as ID
            try:
                channel_id = int(identifier)
                channel = interaction.guild.get_channel(channel_id)
            except ValueError:
                # Try as name (remove # if present)
                search_name = identifier.lstrip('#').lower()
                channel = discord.utils.get(interaction.guild.text_channels, name=search_name)

            if channel:
                channel_ids.append(channel.id)
            else:
                missing_channels.append(identifier)

        if not channel_ids:
            await interaction.response.send_message(
                embed=create_error_embed("None of the provided channels could be found."),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_allowed_channels(self.guild_id, channel_ids, interaction.guild.name):
            channels = [interaction.guild.get_channel(cid) for cid in channel_ids]
            channel_mentions = [ch.mention for ch in channels if ch]

            message = f"Bot commands can now only be used in: {', '.join(channel_mentions)}"
            if missing_channels:
                message += f"\n\n⚠️ Warning: These channels weren't found: {', '.join(missing_channels)}"

            await interaction.response.send_message(
                embed=create_success_embed(message),
                ephemeral=True
            )
            logger.info(f"Allowed channels set to {channel_ids} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update allowed channels."),
                ephemeral=True
            )


class AdminCog(commands.Cog):
    """Cog for admin commands."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager, config):
        """
        Initialize admin cog.

        Args:
            bot: Discord bot instance
            db: Database manager
            config: Bot configuration
        """
        self.bot = bot
        self.db = db
        self.config = config

    @app_commands.command(name="config", description="Configure bot settings (Admin only)")
    async def config(self, interaction: discord.Interaction):
        """
        Display bot configuration interface.

        Args:
            interaction: Discord interaction
        """
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
            return

        # Create embed
        embed = create_embed("Bot Configuration", discord.Color.gold())
        embed.description = (
            "Use the buttons below to configure the bot settings for this server.\n\n"
            "**📢 Set Notification Channel:** Choose where member leave notifications are posted\n"
            "**📅 Set Inactive Days:** Set the threshold for /inactive command\n"
            "**👑 Set Bot Admin Role:** Set which role can manage bot settings\n"
            "**🔐 Toggle User Role Required:** Enable/disable role requirement for using bot commands\n"
            "**👤 Set User Role:** Set which role can use bot commands (when required)\n"
            "**🎯 Set Track Only Roles:** Only track members with specific roles (optional)\n"
            "**📝 Set Allowed Channels:** Restrict bot commands to specific channels (optional)\n"
            "**🔄 Update All Members:** Scan and update all current members in the database\n"
            "**⚙️ View Config:** View current configuration settings"
        )

        # Create view
        view = ConfigView(self.db, interaction.guild_id, self.config)

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        logger.info(f"User {interaction.user} opened config panel in guild {interaction.guild.name}")

    @app_commands.command(name="health", description="Check bot health status (Admin only)")
    async def health(self, interaction: discord.Interaction):
        """
        Display bot health and status information.

        Args:
            interaction: Discord interaction
        """
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Create health status embed
        embed = create_embed("Bot Health Status", discord.Color.green())

        # Bot uptime
        if self.bot.start_time:
            uptime_delta = datetime.now(timezone.utc) - self.bot.start_time
            days = uptime_delta.days
            hours, remainder = divmod(uptime_delta.seconds, 3600)
            minutes, _ = divmod(remainder, 60)

            if days > 0:
                uptime_str = f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                uptime_str = f"{hours}h {minutes}m"
            else:
                uptime_str = f"{minutes}m"
        else:
            uptime_str = "Unknown"

        # Bot latency
        latency_ms = round(self.bot.latency * 1000)

        embed.add_field(
            name="🤖 Bot Status",
            value=f"**Status:** ✅ Online\n**Uptime:** {uptime_str}\n**Latency:** {latency_ms}ms",
            inline=False
        )

        # Database health
        db_health = self.db.get_database_health()
        if db_health['status'] == 'healthy':
            status_icon = "✅"
            status_text = "Healthy"
        else:
            status_icon = "❌"
            status_text = "Unhealthy"

        embed.add_field(
            name="💾 Database",
            value=f"**Status:** {status_icon} {status_text}\n**Size:** {db_health['file_size_mb']} MB",
            inline=False
        )

        # Guild-specific stats
        guild_stats = self.db.get_guild_stats(interaction.guild_id)
        guild_config = self.db.get_guild_config(interaction.guild_id)

        # Check configuration completeness
        config_complete = False
        if guild_config and guild_config['notification_channel_id']:
            config_complete = True

        config_status = "✅ Complete" if config_complete else "⚠️ Incomplete"

        embed.add_field(
            name="🏰 This Guild",
            value=(
                f"**Tracked Members:** {guild_stats['total_members']}\n"
                f"**Active:** {guild_stats['active_members']} | "
                f"**Left:** {guild_stats['inactive_members']}\n"
                f"**Configuration:** {config_status}"
            ),
            inline=False
        )

        await interaction.followup.send(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} checked health status in guild {interaction.guild.name}")

    @app_commands.command(name="server-stats", description="View server activity statistics")
    async def server_stats(self, interaction: discord.Interaction):
        """
        Display server activity statistics and metrics.

        Args:
            interaction: Discord interaction
        """
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            # Get activity statistics
            activity_stats = self.db.get_activity_stats(interaction.guild_id)
            guild_stats = self.db.get_guild_stats(interaction.guild_id)

            # Create embed
            embed = create_embed(f"📊 {interaction.guild.name} - Activity Statistics", discord.Color.blue())

            # Current Status Section
            total_tracked = guild_stats['active_members']
            online_count = activity_stats['currently_online']
            offline_count = activity_stats['currently_offline']

            embed.add_field(
                name="📡 Current Status",
                value=(
                    f"**Total Tracked:** {total_tracked} members\n"
                    f"**🟢 Online:** {online_count}\n"
                    f"**⚫ Offline:** {offline_count}"
                ),
                inline=False
            )

            # Activity Breakdown Section
            embed.add_field(
                name="⏰ Offline Duration Breakdown",
                value=(
                    f"**Last hour:** {activity_stats['offline_1h']} members\n"
                    f"**Last 24 hours:** {activity_stats['offline_24h']} members\n"
                    f"**Last 7 days:** {activity_stats['offline_7d']} members\n"
                    f"**Last 30 days:** {activity_stats['offline_30d']} members\n"
                    f"**30+ days ago:** {activity_stats['offline_30d_plus']} members"
                ),
                inline=False
            )

            # Activity Percentages (if we have members)
            if total_tracked > 0:
                online_pct = round((online_count / total_tracked) * 100, 1)
                recent_pct = round((activity_stats['offline_24h'] / total_tracked) * 100, 1)

                embed.add_field(
                    name="📈 Activity Metrics",
                    value=(
                        f"**Online Rate:** {online_pct}% currently active\n"
                        f"**Recent Activity:** {recent_pct}% seen in last 24h"
                    ),
                    inline=False
                )

            # Member Tracking Info
            left_members = guild_stats['inactive_members']
            embed.add_field(
                name="👥 Member Tracking",
                value=(
                    f"**Active Members:** {guild_stats['active_members']}\n"
                    f"**Left Server:** {left_members}\n"
                    f"**Total Tracked:** {guild_stats['total_members']}"
                ),
                inline=False
            )

            # Create simple text-based bar chart for visualization
            def create_bar(count: int, max_count: int, length: int = 20) -> str:
                if max_count == 0:
                    return "▱" * length
                filled = int((count / max_count) * length)
                return "▰" * filled + "▱" * (length - filled)

            max_offline = max(
                activity_stats['offline_1h'],
                activity_stats['offline_24h'],
                activity_stats['offline_7d'],
                activity_stats['offline_30d'],
                activity_stats['offline_30d_plus'],
                1  # Avoid division by zero
            )

            chart = (
                f"```\n"
                f"Activity Distribution\n"
                f"{'─' * 30}\n"
                f"<1h   {create_bar(activity_stats['offline_1h'], max_offline)} {activity_stats['offline_1h']}\n"
                f"<24h  {create_bar(activity_stats['offline_24h'], max_offline)} {activity_stats['offline_24h']}\n"
                f"<7d   {create_bar(activity_stats['offline_7d'], max_offline)} {activity_stats['offline_7d']}\n"
                f"<30d  {create_bar(activity_stats['offline_30d'], max_offline)} {activity_stats['offline_30d']}\n"
                f"30d+  {create_bar(activity_stats['offline_30d_plus'], max_offline)} {activity_stats['offline_30d_plus']}\n"
                f"```"
            )

            embed.add_field(
                name="📊 Visual Breakdown",
                value=chart,
                inline=False
            )

            embed.set_footer(text=f"Stats generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"User {interaction.user} viewed server stats in guild {interaction.guild.name}")

        except Exception as e:
            logger.error(f"Failed to generate server stats: {e}")
            await interaction.followup.send(
                embed=create_error_embed(f"Failed to generate server statistics: {str(e)}"),
                ephemeral=True
            )

    @app_commands.command(name="help", description="Show bot information and available commands (Admin only)")
    async def help(self, interaction: discord.Interaction):
        """
        Display bot information and list of all available commands.

        Args:
            interaction: Discord interaction
        """
        # Get guild config for bot admin role name
        guild_config = self.db.get_guild_config(interaction.guild_id)
        bot_admin_role_name = guild_config.get('bot_admin_role_name', 'LastSeen Admin') if guild_config else 'LastSeen Admin'

        # Check permissions
        if not has_bot_admin_role(interaction.user, bot_admin_role_name):
            await interaction.response.send_message(
                embed=create_error_embed(
                    f"You need the '{bot_admin_role_name}' role or Administrator permission to use this command."
                ),
                ephemeral=True
            )
            return

        # Create help embed
        embed = create_embed("LastSeen Bot - Help", discord.Color.blue())
        embed.description = (
            "Track user activity, monitor presence, and manage member data across your server."
        )

        # User Commands
        embed.add_field(
            name="👥 User Commands",
            value=(
                "`/whois <user>` - Show user info, roles, join date, last seen\n"
                "`/lastseen <user>` or `/seen <user>` - When user was last online\n"
                "`/inactive` - List members offline beyond threshold\n"
                "`/server-stats` - Server activity metrics and charts"
            ),
            inline=False
        )

        # Admin Commands
        embed.add_field(
            name="⚙️ Admin Commands",
            value=(
                "`/config` - Configure bot settings:\n"
                "  • Notification channel & inactive days\n"
                "  • Admin/user role permissions\n"
                "  • Track only specific roles (optional)\n"
                "  • Restrict commands to channels (optional)\n"
                "  • Update member database\n"
                "`/health` - Bot status & database health\n"
                "`/help` - Show this help message"
            ),
            inline=False
        )

        # Permissions & Features
        embed.add_field(
            name="🔐 Permissions & Features",
            value=(
                "**Access Control**\n"
                "• Admin: Bot Admin role or Administrator\n"
                "• Users: Configurable role & channel restrictions\n\n"
                "**Key Features**\n"
                "• Presence tracking • Activity statistics\n"
                "• Role-based visibility • Channel restrictions\n"
                "• Multi-guild support • Privacy focused"
            ),
            inline=False
        )

        # Footer with helpful info
        embed.set_footer(text=f"Bot Admin Role: {bot_admin_role_name}")

        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info(f"User {interaction.user} viewed help in guild {interaction.guild.name}")


async def setup(bot: commands.Bot):
    """
    Setup function for loading the cog.

    Args:
        bot: Discord bot instance
    """
    db = bot.db
    config = bot.config
    await bot.add_cog(AdminCog(bot, db, config))
    logger.info("AdminCog loaded")

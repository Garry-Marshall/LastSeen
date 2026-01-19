"""Interactive configuration view for bot settings."""

import discord
import logging
import json

from database import DatabaseManager
from bot.utils import create_embed, create_error_embed, create_success_embed
from .permissions import get_bot_admin_role_name, check_admin_permission
from .channel_config import ChannelModal, InactiveDaysModal
from .role_config import BotAdminRoleModal, UserRoleModal, TrackOnlyRolesModal
from .channel_filter import AllowedChannelsModal
from . import member_mgmt

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

        enabled = bool(guild_config.get("user_role_required", 0))
        self.toggle_user_role_required.label = (f"User Role Required: {'ON' if enabled else 'OFF'}")


    @discord.ui.button(label="Set Notification Channel", style=discord.ButtonStyle.primary, emoji="📢", row=0)
    async def set_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the notification channel."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for channel input
        modal = ChannelModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Inactive Days", style=discord.ButtonStyle.primary, emoji="📅", row=0)
    async def set_inactive_days(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the inactive days threshold."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for days input
        modal = InactiveDaysModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Update All Members", style=discord.ButtonStyle.success, emoji="🔄", row=0)
    async def update_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to enumerate and update all members."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await member_mgmt.update_all_members(interaction, self.db)

    @discord.ui.button(label="Set Bot Admin Role", style=discord.ButtonStyle.primary, emoji="👑", row=1)
    async def set_bot_admin_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the bot admin role name."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for bot admin role input
        modal = BotAdminRoleModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set User Role", style=discord.ButtonStyle.primary, emoji="👤", row=1)
    async def set_user_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the user role name."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for user role input
        modal = UserRoleModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="User Role Required: OFF", emoji="🔐", row=1)
    async def toggle_user_role_required(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to toggle user role requirement."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
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
        #new_value = not current_value
        new_value = not bool(guild_config.get("user_role_required", 0))

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
            # Update button style dynamically for the current view instance - Note: Ephemeral messages can't be edited after sending
            #button.style = discord.ButtonStyle.success if new_value else discord.ButtonStyle.danger
            #await interaction.message.edit(view=self)
            button.label = f"User Role Required: {'ON' if new_value else 'OFF'}"
            button.style = (discord.ButtonStyle.success if new_value else discord.ButtonStyle.danger)

            await interaction.edit_original_response(view=self) 

            logger.info(f"User role requirement toggled to {new_value} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed("Failed to update user role requirement."),
                ephemeral=True
            )

    @discord.ui.button(label="Set Track Only Roles", style=discord.ButtonStyle.primary, emoji="🎯", row=2)
    async def set_track_only_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set which roles to track (optional)."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for track only roles input
        modal = TrackOnlyRolesModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Allowed Channels", style=discord.ButtonStyle.primary, emoji="📝", row=2)
    async def set_allowed_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set which channels can use commands (optional)."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
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

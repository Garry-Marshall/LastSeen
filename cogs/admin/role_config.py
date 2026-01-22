"""Role-based access control and filtering modals."""

import discord
import logging
import json

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed

logger = logging.getLogger(__name__)


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
                embed=create_error_embed(
                    "Failed to update bot admin role. "
                    "Database error occurred. Please try again or contact support."
                ),
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
                embed=create_error_embed(
                    "Failed to update user role. "
                    "Database error occurred. Please try again or contact support."
                ),
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

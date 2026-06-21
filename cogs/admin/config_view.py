"""Interactive configuration view for bot settings."""

import discord
import logging
import json

from database import DatabaseManager
from bot.utils import create_embed, create_error_embed, create_success_embed
from bot.locale import t, guild_language, available_languages, language_name, weekday_name
from .permissions import get_bot_admin_role_name, check_admin_permission
from .channel_config import ChannelModal, InactiveDaysModal, TimezoneModal, ReportsConfigModal
from .role_config import BotAdminRoleModal, UserRoleModal, TrackOnlyRolesModal
from .channel_filter import AllowedChannelsModal
from .quick_setup import QuickSetupView
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
        self.lang = guild_language(guild_config)
        lang = self.lang

        # Localize static button labels (emoji set via the decorators is preserved)
        self.set_channel.label = t("admin.config_view.btn_set_channel", lang)
        self.set_inactive_days.label = t("admin.config_view.btn_set_inactive_days", lang)
        self.set_timezone.label = t("admin.config_view.btn_set_timezone", lang)
        self.set_language.label = t("admin.config_view.btn_set_language", lang)
        self.set_bot_admin_role.label = t("admin.config_view.btn_set_bot_admin_role", lang)
        self.set_user_role.label = t("admin.config_view.btn_set_user_role", lang)
        self.set_track_only_roles.label = t("admin.config_view.btn_set_track_only_roles", lang)
        self.set_allowed_channels.label = t("admin.config_view.btn_set_allowed_channels", lang)
        self.configure_reports.label = t("admin.config_view.btn_configure_reports", lang)
        self.disable_reports.label = t("admin.config_view.btn_disable_reports", lang)
        self.quick_setup.label = t("admin.config_view.btn_quick_setup", lang)
        self.view_config.label = t("admin.config_view.btn_view_config", lang)

        user_role_required = guild_config.get('user_role_required', 0) if guild_config else 0
        # Set button style based on state: green if enabled, red if disabled
        self.toggle_user_role_required.style = discord.ButtonStyle.success if user_role_required else discord.ButtonStyle.danger
        self.toggle_user_role_required.label = t(
            "admin.config_view.toggle_user_role", lang,
            state=t('common.on' if user_role_required else 'common.off', lang)
        )


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

    @discord.ui.button(label="Set Timezone", style=discord.ButtonStyle.primary, emoji="🌍", row=0)
    async def set_timezone(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set server timezone."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for timezone input
        modal = TimezoneModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Language", style=discord.ButtonStyle.primary, emoji="🌐", row=0)
    async def set_language(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to set the bot's language for this server."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        view = LanguageSelectView(self.db, self.guild_id)
        embed = create_embed(t("admin.language.prompt_title", self.lang), discord.Color.blurple())
        embed.description = t("admin.language.prompt_desc", self.lang)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

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

    @discord.ui.button(label="User Role Required = OFF", emoji="🔐", row=1)
    async def toggle_user_role_required(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to toggle user role requirement."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        lang = self.lang

        # Get current config
        guild_config = self.db.get_guild_config(self.guild_id)
        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.errors.guild_config_not_found", lang), lang),
                ephemeral=True
            )
            return

        # Toggle the setting
        current_value = guild_config.get('user_role_required', 0)
        #new_value = not current_value
        new_value = not bool(guild_config.get("user_role_required", 0))

        # Update database
        if self.db.set_user_role_required(self.guild_id, new_value, interaction.guild.name):
            # Update the toggle button on the config panel itself. Responding to the
            # component interaction with edit_message targets the panel message
            # (works for ephemeral messages too).
            button.label = t(
                "admin.config_view.toggle_user_role", lang,
                state=t('common.on' if new_value else 'common.off', lang)
            )
            button.style = (discord.ButtonStyle.success if new_value else discord.ButtonStyle.danger)
            await interaction.response.edit_message(view=self)

            message = t(
                "admin.config_view.user_role_enabled" if new_value else "admin.config_view.user_role_disabled",
                lang
            )
            await interaction.followup.send(
                embed=create_success_embed(message, lang),
                ephemeral=True
            )

            logger.info(f"User role requirement toggled to {new_value} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.config_view.toggle_failed", lang), lang),
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

    @discord.ui.button(label="Configure Reports", style=discord.ButtonStyle.primary, emoji="📊", row=3)
    async def configure_reports(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to configure scheduled reports."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create modal for reports configuration
        modal = ReportsConfigModal(self.db, self.guild_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Disable Reports", style=discord.ButtonStyle.danger, emoji="⏹️", row=3)
    async def disable_reports(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to disable scheduled reports."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        lang = self.lang

        # Check if reports are currently enabled
        guild_config = self.db.get_guild_config(self.guild_id)
        if not guild_config or not guild_config.get('report_frequency'):
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.config_view.reports_not_enabled", lang), lang),
                ephemeral=True
            )
            return

        # Show confirmation dialog
        confirmation_view = DisableReportsConfirmView(self.db, self.guild_id)
        embed = create_embed(t("admin.config_view.confirm_disable_title", lang), discord.Color.orange())
        embed.description = t("admin.config_view.confirm_disable_desc", lang)

        await interaction.response.send_message(
            embed=embed,
            view=confirmation_view,
            ephemeral=True
        )

    @discord.ui.button(label="🚀 Quick Setup", style=discord.ButtonStyle.primary, row=4)
    async def quick_setup(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Launch quick setup wizard."""
        if not await check_admin_permission(interaction, self.db, self.guild_id):
            return

        # Create wizard view
        wizard_view = QuickSetupView(self.db, self.guild_id, self.config)
        embed = wizard_view._get_step_embed()
        
        await interaction.response.send_message(
            embed=embed,
            view=wizard_view,
            ephemeral=True
        )
        logger.info(f"Quick setup wizard started for guild {interaction.guild.name}")

    @discord.ui.button(label="View Config", style=discord.ButtonStyle.secondary, emoji="⚙️", row=4)
    async def view_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Button to view current configuration."""
        lang = self.lang
        guild_config = self.db.get_guild_config(self.guild_id)

        if not guild_config:
            await interaction.response.send_message(
                embed=create_error_embed(t("admin.errors.guild_config_not_found", lang), lang),
                ephemeral=True
            )
            return

        embed = create_embed(t("admin.view_config.title", lang), discord.Color.blue())

        # Notification channel
        channel_id = guild_config['notification_channel_id']
        if channel_id:
            channel = interaction.guild.get_channel(channel_id)
            channel_str = channel.mention if channel else t("common.unknown_with_id", lang, id=channel_id)
        else:
            channel_str = t("common.not_set", lang)
        embed.add_field(name=t("admin.view_config.notification_channel", lang), value=channel_str, inline=False)

        # Inactive days
        embed.add_field(
            name=t("admin.view_config.inactive_days", lang),
            value=t("admin.view_config.days_value", lang, days=guild_config['inactive_days']),
            inline=False
        )

        # Timezone
        timezone = guild_config.get('timezone', 'UTC')
        embed.add_field(
            name=t("admin.view_config.timezone", lang),
            value=timezone,
            inline=False
        )

        # Scheduled Reports
        report_frequency = guild_config.get('report_frequency')
        if report_frequency:
            report_channel_id = guild_config.get('report_channel_id')
            report_channel = interaction.guild.get_channel(report_channel_id) if report_channel_id else None
            channel_mention = report_channel.mention if report_channel else f"Unknown ({report_channel_id})"
            
            report_types = guild_config.get('report_types', '[]')
            try:
                types_list = json.loads(report_types) if report_types else []
                types_str = ', '.join(types_list) if types_list else t("admin.view_config.types_none", lang)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to parse report_types JSON for guild {self.guild_id}")
                types_str = t("admin.view_config.types_invalid", lang)

            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            schedule_info = ""
            time_hour = guild_config.get('report_time_hour', 9)
            guild_tz_str = guild_config.get('timezone', 'UTC')
            if report_frequency == 'weekly':
                day_weekly = guild_config.get('report_day_weekly', 0)
                schedule_info = t("admin.view_config.schedule_weekly", lang, day=weekday_name(day_names[day_weekly], lang), daynum=day_weekly + 1, hour=time_hour, tz=guild_tz_str)
            elif report_frequency == 'monthly':
                day_monthly = guild_config.get('report_day_monthly', 1)
                schedule_info = t("admin.view_config.schedule_monthly", lang, day=day_monthly, hour=time_hour, tz=guild_tz_str)
            else:  # both
                day_weekly = guild_config.get('report_day_weekly', 0)
                day_monthly = guild_config.get('report_day_monthly', 1)
                schedule_info = t("admin.view_config.schedule_both", lang, day=weekday_name(day_names[day_weekly], lang), daynum=day_weekly + 1, monthday=day_monthly, hour=time_hour, tz=guild_tz_str)

            embed.add_field(
                name=t("admin.view_config.scheduled_reports", lang),
                value=t("admin.view_config.reports_enabled_value", lang, channel=channel_mention, schedule=schedule_info, types=types_str),
                inline=False
            )
        else:
            embed.add_field(
                name=t("admin.view_config.scheduled_reports", lang),
                value=t("admin.view_config.reports_disabled_value", lang),
                inline=False
            )

        # Bot admin role
        bot_admin_role = guild_config.get('bot_admin_role_name', 'LastSeen Admin')
        embed.add_field(name=t("admin.view_config.bot_admin_role", lang), value=bot_admin_role, inline=False)

        # User role required
        user_role_required = guild_config.get('user_role_required', 0)
        user_role_required_str = t("common.yes", lang) if user_role_required else t("common.no", lang)
        embed.add_field(name=t("admin.view_config.user_role_required", lang), value=user_role_required_str, inline=False)

        # User role name
        user_role_name = guild_config.get('user_role_name', 'LastSeen User')
        embed.add_field(name=t("admin.view_config.user_role_name", lang), value=user_role_name, inline=False)

        # Track only roles
        track_only_roles = guild_config.get('track_only_roles')
        if track_only_roles:
            try:
                roles_list = json.loads(track_only_roles)
                track_only_str = ", ".join(roles_list) if roles_list else t("admin.view_config.all_roles", lang)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to parse track_only_roles JSON")
                track_only_str = t("admin.view_config.error_parsing_roles", lang)
        else:
            track_only_str = t("admin.view_config.all_roles", lang)
        embed.add_field(name=t("admin.view_config.track_only_roles", lang), value=track_only_str, inline=False)

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
                        else:
                            channel_mentions.append(t("common.unknown_with_id", lang, id=ch_id))
                    allowed_channels_str = ", ".join(channel_mentions)
                else:
                    allowed_channels_str = t("admin.view_config.all_channels", lang)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Failed to parse allowed_channels JSON")
                allowed_channels_str = t("admin.view_config.error_parsing_channels", lang)
        else:
            allowed_channels_str = t("admin.view_config.all_channels", lang)
        embed.add_field(name=t("admin.view_config.allowed_channels", lang), value=allowed_channels_str, inline=False)

        # Member count
        member_count = len(self.db.get_all_guild_members(self.guild_id))
        embed.add_field(name=t("admin.view_config.tracked_members", lang), value=str(member_count), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


class DisableReportsConfirmView(discord.ui.View):
    """Confirmation view for disabling scheduled reports."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        super().__init__(timeout=60)
        self.db = db
        self.guild_id = guild_id
        self.lang = guild_language(db.get_guild_config(guild_id))
        self.confirm_disable.label = t("admin.config_view.btn_confirm_disable", self.lang)
        self.cancel_disable.label = t("common.cancel", self.lang)

    @discord.ui.button(label="Yes, Disable Reports", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_disable(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Confirm disabling reports."""
        lang = self.lang
        success = self.db.disable_reports(self.guild_id)
        if success:
            await interaction.response.edit_message(
                embed=create_success_embed(t("admin.config_view.reports_disabled_success", lang), lang),
                view=None
            )
        else:
            await interaction.response.edit_message(
                embed=create_error_embed(t("admin.config_view.disable_failed", lang), lang),
                view=None
            )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_disable(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the disable operation."""
        lang = self.lang
        embed = create_embed(t("common.cancelled", lang), discord.Color.blue())
        embed.description = t("admin.config_view.disable_cancelled_desc", lang)
        await interaction.response.edit_message(
            embed=embed,
            view=None
        )
        self.stop()


class LanguageSelectView(discord.ui.View):
    """Dropdown for choosing the bot's language for this server.

    Options are populated from every loaded locale, so adding a new
    locales/<code>.json file makes it appear here with no code change.
    """

    def __init__(self, db: DatabaseManager, guild_id: int):
        super().__init__(timeout=120)
        self.db = db
        self.guild_id = guild_id
        guild_config = db.get_guild_config(guild_id)
        self.lang = guild_language(guild_config)
        current = guild_config.get('language', 'en') if guild_config else 'en'

        options = [
            discord.SelectOption(label=language_name(code), value=code, default=(code == current))
            for code in available_languages()
        ]
        self.language_select = discord.ui.Select(
            placeholder=t("admin.language.select_placeholder", self.lang),
            options=options,
            min_values=1,
            max_values=1
        )
        self.language_select.callback = self._on_select
        self.add_item(self.language_select)

    async def _on_select(self, interaction: discord.Interaction):
        """Persist the chosen language and confirm in that new language."""
        new_lang = self.language_select.values[0]
        if not self.db.set_guild_language(self.guild_id, new_lang, interaction.guild.name):
            await interaction.response.edit_message(
                embed=create_error_embed(t("admin.language.update_failed", new_lang), new_lang),
                view=None
            )
            return

        await interaction.response.edit_message(
            embed=create_success_embed(t("admin.language.set", new_lang, language=language_name(new_lang)), new_lang),
            view=None
        )
        logger.info(f"Language set to '{new_lang}' in guild {interaction.guild.name}")

"""Quick Setup wizard for first-time configuration."""

import discord
import logging
from typing import Optional

from database import DatabaseManager
from bot.utils import create_embed, create_success_embed
from bot.locale import t, guild_language
from .channel_config import ChannelModal, InactiveDaysModal, TimezoneModal
from .role_config import BotAdminRoleModal

logger = logging.getLogger(__name__)


class QuickSetupView(discord.ui.View):
    """Interactive quick setup wizard with paginated steps."""

    def __init__(self, db: DatabaseManager, guild_id: int, config):
        """
        Initialize quick setup view.

        Args:
            db: Database manager
            guild_id: Discord guild ID
            config: Bot configuration
        """
        super().__init__(timeout=600)  # 10 minute timeout for setup
        self.db = db
        self.guild_id = guild_id
        self.config = config
        self.lang = guild_language(db.get_guild_config(guild_id))
        self.current_step = 0
        self.max_steps = 5

        # Track completed steps
        self.completed = {
            'channel': False,
            'inactive_days': False,
            'admin_role': False,
            'timezone': False,
            'summary': False
        }

        # Localize static button labels (next_button label is set in _update_buttons)
        self.prev_button.label = t("admin.quick_setup.btn_prev", self.lang)
        self.skip_button.label = t("admin.quick_setup.btn_skip", self.lang)
        self.configure_button.label = t("admin.quick_setup.btn_configure", self.lang)
        self.cancel_button.label = t("admin.quick_setup.btn_cancel", self.lang)

        self._update_buttons()

    def _update_buttons(self):
        """Update button states based on current step."""
        # Previous button
        self.prev_button.disabled = (self.current_step == 0)

        # Next button - show "Finish" on last step
        if self.current_step == self.max_steps - 1:
            self.next_button.label = t("admin.quick_setup.btn_finish", self.lang)
            self.next_button.style = discord.ButtonStyle.success
        else:
            self.next_button.label = t("admin.quick_setup.btn_next", self.lang)
            self.next_button.style = discord.ButtonStyle.primary
        
        # Configure button - hide on summary page
        self.configure_button.disabled = (self.current_step == self.max_steps - 1)
        
        # Skip button - only show on optional steps: admin role (2) and timezone (3)
        self.skip_button.disabled = self.current_step not in [2, 3]

    def _get_step_embed(self) -> discord.Embed:
        """Get embed for current step."""
        step_embeds = [
            self._step_1_notification_channel(),
            self._step_2_inactive_days(),
            self._step_3_admin_role(),
            self._step_4_timezone(),
            self._step_5_summary()
        ]
        return step_embeds[self.current_step]

    def _step_1_notification_channel(self) -> discord.Embed:
        """Step 1: Set notification channel."""
        embed = create_embed(t("admin.quick_setup.step1_title", self.lang), discord.Color.blue())
        embed.description = t("admin.quick_setup.step1_desc", self.lang, channel=self._get_current_channel())
        embed.set_footer(text=t("admin.quick_setup.step1_footer", self.lang))
        return embed

    def _step_2_inactive_days(self) -> discord.Embed:
        """Step 2: Set inactive days threshold."""
        embed = create_embed(t("admin.quick_setup.step2_title", self.lang), discord.Color.blue())
        embed.description = t("admin.quick_setup.step2_desc", self.lang, days=self._get_current_inactive_days())
        embed.set_footer(text=t("admin.quick_setup.step2_footer", self.lang))
        return embed

    def _step_3_admin_role(self) -> discord.Embed:
        """Step 3: Set bot admin role."""
        embed = create_embed(t("admin.quick_setup.step3_title", self.lang), discord.Color.blue())
        embed.description = t("admin.quick_setup.step3_desc", self.lang, role=self._get_current_admin_role())
        embed.set_footer(text=t("admin.quick_setup.step3_footer", self.lang))
        return embed

    def _step_4_timezone(self) -> discord.Embed:
        """Step 4: Set server timezone (optional)."""
        embed = create_embed(t("admin.quick_setup.step4_title", self.lang), discord.Color.gold())
        embed.description = t("admin.quick_setup.step4_desc", self.lang, timezone=self._get_current_timezone())
        embed.set_footer(text=t("admin.quick_setup.step4_footer", self.lang))
        return embed

    def _step_5_summary(self) -> discord.Embed:
        """Step 5: Setup summary."""
        embed = create_embed(t("admin.quick_setup.step5_title", self.lang), discord.Color.green())

        config = self.db.get_guild_config(self.guild_id)
        if config:
            summary = t(
                "admin.quick_setup.step5_summary", self.lang,
                channel=self._get_current_channel(),
                days=config['inactive_days'],
                role=config.get('bot_admin_role_name', 'LastSeen Admin'),
                timezone=config.get('timezone', 'UTC')
            )
        else:
            summary = t("admin.quick_setup.step5_no_config", self.lang)

        embed.description = summary
        embed.set_footer(text=t("admin.quick_setup.step5_footer", self.lang))
        return embed

    def _get_current_channel(self) -> str:
        """Get current notification channel setting."""
        config = self.db.get_guild_config(self.guild_id)
        if config and config.get('notification_channel_id'):
            return f"<#{config['notification_channel_id']}>"
        return t("common.not_set", self.lang)

    def _get_current_inactive_days(self) -> int:
        """Get current inactive days setting."""
        config = self.db.get_guild_config(self.guild_id)
        return config['inactive_days'] if config else 10

    def _get_current_admin_role(self) -> str:
        """Get current admin role setting."""
        config = self.db.get_guild_config(self.guild_id)
        return config.get('bot_admin_role_name', 'LastSeen Admin') if config else 'LastSeen Admin'

    def _get_current_timezone(self) -> str:
        """Get current timezone setting."""
        config = self.db.get_guild_config(self.guild_id)
        return config.get('timezone', 'UTC') if config else 'UTC'

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary, row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous step."""
        self.current_step = max(0, self.current_step - 1)
        self._update_buttons()
        embed = self._get_step_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.primary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next step or finish."""
        if self.current_step == self.max_steps - 1:
            # Finish setup
            embed = create_success_embed(t("admin.quick_setup.finished", self.lang), self.lang)
            await interaction.response.edit_message(embed=embed, view=None)
            logger.info(f"Quick setup completed for guild {self.guild_id}")
        else:
            self.current_step = min(self.max_steps - 1, self.current_step + 1)
            self._update_buttons()
            embed = self._get_step_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.secondary, row=0)
    async def skip_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Skip optional step."""
        self.current_step = min(self.max_steps - 1, self.current_step + 1)
        self._update_buttons()
        embed = self._get_step_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⚙️ Configure", style=discord.ButtonStyle.success, row=1)
    async def configure_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open configuration modal for current step."""
        modal = None
        
        if self.current_step == 0:
            modal = ChannelModal(self.db, self.guild_id)
            self.completed['channel'] = True
        elif self.current_step == 1:
            modal = InactiveDaysModal(self.db, self.guild_id)
            self.completed['inactive_days'] = True
        elif self.current_step == 2:
            modal = BotAdminRoleModal(self.db, self.guild_id)
            self.completed['admin_role'] = True
        elif self.current_step == 3:
            modal = TimezoneModal(self.db, self.guild_id)
            self.completed['timezone'] = True
        
        if modal:
            await interaction.response.send_modal(modal)
            # Refresh the embed after modal closes (user will click next manually)

    @discord.ui.button(label="❌ Cancel Setup", style=discord.ButtonStyle.danger, row=1)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel setup wizard."""
        embed = create_embed(t("admin.quick_setup.cancelled_title", self.lang), discord.Color.red())
        embed.description = t("admin.quick_setup.cancelled_desc", self.lang)
        await interaction.response.edit_message(embed=embed, view=None)
        logger.info(f"Quick setup cancelled for guild {self.guild_id}")

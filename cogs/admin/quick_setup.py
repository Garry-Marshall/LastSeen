"""Quick Setup wizard for first-time configuration."""

import discord
import logging
from typing import Optional

from database import DatabaseManager
from bot.utils import create_embed, create_success_embed
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
        
        self._update_buttons()

    def _update_buttons(self):
        """Update button states based on current step."""
        # Previous button
        self.prev_button.disabled = (self.current_step == 0)
        
        # Next button - show "Finish" on last step
        if self.current_step == self.max_steps - 1:
            self.next_button.label = "Finish Setup"
            self.next_button.style = discord.ButtonStyle.success
        else:
            self.next_button.label = "Next ▶️"
            self.next_button.style = discord.ButtonStyle.primary
        
        # Configure button - hide on summary page
        self.configure_button.disabled = (self.current_step == self.max_steps - 1)
        
        # Skip button - only show on optional steps (3, 4)
        self.skip_button.disabled = self.current_step not in [3, 4]

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
        embed = create_embed("🚀 Quick Setup - Step 1/5", discord.Color.blue())
        embed.description = (
            "**📢 Notification Channel**\n\n"
            "Choose where the bot will post notifications when members leave your server.\n\n"
            "**Why this matters:**\n"
            "• Get alerts when members depart\n"
            "• Track who left and when\n"
            "• Keep admins informed of changes\n\n"
            "**Recommendation:** Use a private admin channel or mod logs channel.\n\n"
            f"**Current setting:** {self._get_current_channel()}"
        )
        embed.set_footer(text="Click 'Configure' to set this up, or 'Next' to skip for now")
        return embed

    def _step_2_inactive_days(self) -> discord.Embed:
        """Step 2: Set inactive days threshold."""
        embed = create_embed("🚀 Quick Setup - Step 2/5", discord.Color.blue())
        embed.description = (
            "**📅 Inactive Days Threshold**\n\n"
            "Set how many days a member must be offline before appearing in `/inactive` command.\n\n"
            "**Why this matters:**\n"
            "• Identify members who may have lost interest\n"
            "• Plan engagement campaigns\n"
            "• Clean up inactive roles\n\n"
            "**Common values:**\n"
            "• Small communities: 7-14 days\n"
            "• Medium servers: 30 days\n"
            "• Large servers: 60-90 days\n\n"
            f"**Current setting:** {self._get_current_inactive_days()} days"
        )
        embed.set_footer(text="Click 'Configure' to change this value")
        return embed

    def _step_3_admin_role(self) -> discord.Embed:
        """Step 3: Set bot admin role."""
        embed = create_embed("🚀 Quick Setup - Step 3/5", discord.Color.blue())
        embed.description = (
            "**👑 Bot Admin Role**\n\n"
            "Choose which role can manage bot settings and access admin commands.\n\n"
            "**Why this matters:**\n"
            "• Controls who can configure the bot\n"
            "• Restricts access to sensitive commands\n"
            "• Doesn't require Discord Administrator permission\n\n"
            "**Recommendation:** Use your existing 'Moderator' or 'Admin' role.\n\n"
            f"**Current setting:** {self._get_current_admin_role()}"
        )
        embed.set_footer(text="Click 'Configure' to set this up, or 'Skip' to use default")
        return embed

    def _step_4_timezone(self) -> discord.Embed:
        """Step 4: Set server timezone (optional)."""
        embed = create_embed("🚀 Quick Setup - Step 4/5", discord.Color.gold())
        embed.description = (
            "**🌍 Server Timezone (Optional)**\n\n"
            "Set your server's timezone for accurate timestamp display in all commands.\n\n"
            "**Why this matters:**\n"
            "• Shows times in your local timezone\n"
            "• Makes activity reports more meaningful\n"
            "• Improves readability for your community\n\n"
            "**Examples:**\n"
            "• US East Coast: `America/New_York`\n"
            "• UK: `Europe/London`\n"
            "• Japan: `Asia/Tokyo`\n"
            "• Australia: `Australia/Sydney`\n\n"
            f"**Current setting:** {self._get_current_timezone()}"
        )
        embed.set_footer(text="Optional: Configure timezone or Skip to use UTC")
        return embed

    def _step_5_summary(self) -> discord.Embed:
        """Step 5: Setup summary."""
        embed = create_embed("✅ Quick Setup Complete!", discord.Color.green())
        
        config = self.db.get_guild_config(self.guild_id)
        if config:
            summary = (
                "**Your configuration:**\n\n"
                f"📢 **Notification Channel:** {self._get_current_channel()}\n"
                f"📅 **Inactive Threshold:** {config['inactive_days']} days\n"
                f"👑 **Bot Admin Role:** {config.get('bot_admin_role_name', 'LastSeen Admin')}\n"
                f"🌍 **Timezone:** {config.get('timezone', 'UTC')}\n\n"
                "**Next steps:**\n"
                "• Test the bot with `/whois @user` or `/inactive`\n"
                "• Use `/config` to access advanced settings\n"
                "• Check `/help` for all available commands\n"
                "• Set up scheduled reports (optional)\n"
            )
        else:
            summary = "⚠️ Configuration not found. Please try setup again."
        
        embed.description = summary
        embed.set_footer(text="Click 'Finish Setup' to close this wizard")
        return embed

    def _get_current_channel(self) -> str:
        """Get current notification channel setting."""
        config = self.db.get_guild_config(self.guild_id)
        if config and config.get('notification_channel_id'):
            return f"<#{config['notification_channel_id']}>"
        return "Not set"

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
            embed = create_success_embed(
                "Setup wizard completed! Your bot is ready to use.\n\n"
                "Use `/config` anytime to adjust settings or explore advanced features."
            )
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
        embed = create_embed("Setup Cancelled", discord.Color.red())
        embed.description = "Quick setup was cancelled. You can use `/config` anytime to configure the bot."
        await interaction.response.edit_message(embed=embed, view=None)
        logger.info(f"Quick setup cancelled for guild {self.guild_id}")

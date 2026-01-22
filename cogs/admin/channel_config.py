"""Channel and timing configuration modals."""

import discord
import logging
import pytz

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed

logger = logging.getLogger(__name__)


class RetentionDaysModal(discord.ui.Modal, title="Set Message Retention Days"):
    """Modal for setting message activity retention period."""

    days_input = discord.ui.TextInput(
        label="Days to Retain Message Activity",
        placeholder="e.g., 365 (default), 180, 730",
        required=True,
        max_length=4
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
        
        # Pre-fill with current value
        guild_config = db.get_guild_config(guild_id)
        if guild_config and guild_config.get('message_retention_days'):
            self.days_input.default = str(guild_config['message_retention_days'])

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        try:
            days = int(self.days_input.value.strip())
            
            if days < 30:
                await interaction.response.send_message(
                    embed=create_error_embed("Retention period must be at least 30 days."),
                    ephemeral=True
                )
                return
            
            if days > 3650:  # 10 years
                await interaction.response.send_message(
                    embed=create_error_embed("Retention period cannot exceed 3650 days (10 years)."),
                    ephemeral=True
                )
                return
            
            # Update database
            self.db.set_message_retention_days(self.guild_id, days, interaction.guild.name)
            
            await interaction.response.send_message(
                embed=create_success_embed(
                    f"✅ Message retention period set to **{days} days**.\n\n"
                    f"Message activity older than {days} days will be automatically deleted during daily cleanup."
                ),
                ephemeral=True
            )
            logger.info(f"Message retention set to {days} days for guild {self.guild_id}")
            
        except ValueError:
            await interaction.response.send_message(
                embed=create_error_embed("Invalid number. Please enter a valid number of days."),
                ephemeral=True
            )


class TimezoneModal(discord.ui.Modal, title="Set Server Timezone"):
    """Modal for setting the server timezone."""

    timezone_input = discord.ui.TextInput(
        label="Timezone (e.g., America/New_York)",
        placeholder="Use IANA timezone format",
        required=True,
        max_length=50
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
        
        # Pre-fill with current value
        guild_config = db.get_guild_config(guild_id)
        if guild_config and guild_config.get('timezone'):
            self.timezone_input.default = guild_config['timezone']

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        try:
            timezone_str = self.timezone_input.value.strip()
            
            # Validate timezone
            if timezone_str not in pytz.all_timezones:
                # Try to find close matches
                timezone_lower = timezone_str.lower()
                suggestions = [tz for tz in pytz.all_timezones if timezone_lower in tz.lower()][:5]
                
                error_msg = f"Invalid timezone: `{timezone_str}`"
                if suggestions:
                    error_msg += f"\n\nDid you mean one of these?\n" + "\n".join(f"• `{tz}`" for tz in suggestions)
                else:
                    error_msg += "\n\nPlease use IANA timezone format (e.g., `America/New_York`, `Europe/London`, `Asia/Tokyo`)."
                    error_msg += "\n\nSee full list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
                
                await interaction.response.send_message(
                    embed=create_error_embed(error_msg),
                    ephemeral=True
                )
                return
            
            # Update database
            self.db.set_timezone(self.guild_id, timezone_str, interaction.guild.name)
            
            await interaction.response.send_message(
                embed=create_success_embed(
                    f"✅ Server timezone set to **{timezone_str}**.\n\n"
                    f"All timestamps will now be displayed in this timezone."
                ),
                ephemeral=True
            )
            logger.info(f"Timezone set to {timezone_str} for guild {self.guild_id}")
            
        except Exception as e:
            await interaction.response.send_message(
                embed=create_error_embed(f"Error setting timezone: {str(e)}"),
                ephemeral=True
            )
            logger.error(f"Failed to set timezone for guild {self.guild_id}: {e}")


class ReportsConfigModal(discord.ui.Modal, title="Configure Scheduled Reports"):
    """Modal for configuring scheduled reports."""

    channel_input = discord.ui.TextInput(
        label="Report Channel (name, ID, or mention)",
        placeholder="e.g., reports, #reports, or channel ID",
        required=True,
        max_length=100
    )

    frequency_input = discord.ui.TextInput(
        label="Frequency (weekly, monthly, or both)",
        placeholder="weekly, monthly, or both",
        required=True,
        max_length=10
    )

    report_types_input = discord.ui.TextInput(
        label="Report Types (comma-separated)",
        placeholder="activity, members, departures",
        required=True,
        max_length=50,
        default="activity,members,departures"
    )

    day_input = discord.ui.TextInput(
        label="Day (0-6 for weekly, 1-28 for monthly)",
        placeholder="Weekly: 0=Mon, 6=Sun | Monthly: 1-28",
        required=False,
        max_length=2
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
        
        # Pre-fill with current values if configured
        guild_config = db.get_guild_config(guild_id)
        if guild_config:
            if guild_config.get('report_channel_id'):
                self.channel_input.default = str(guild_config['report_channel_id'])
            if guild_config.get('report_frequency'):
                self.frequency_input.default = guild_config['report_frequency']
            if guild_config.get('report_types'):
                import json
                try:
                    types = json.loads(guild_config['report_types'])
                    self.report_types_input.default = ','.join(types)
                except (json.JSONDecodeError, TypeError):
                    logger.error(f"Failed to parse report_types JSON for guild {guild_id}")
                    pass

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        try:
            # Parse channel
            channel_str = self.channel_input.value.strip()
            channel = None

            # Try different parsing methods
            if channel_str.startswith('<#') and channel_str.endswith('>'):
                try:
                    channel_id = int(channel_str[2:-1])
                    channel = interaction.guild.get_channel(channel_id)
                except ValueError:
                    pass

            if not channel:
                try:
                    channel_id = int(channel_str)
                    channel = interaction.guild.get_channel(channel_id)
                except ValueError:
                    pass

            if not channel:
                search_name = channel_str.lstrip('#').lower()
                channel = discord.utils.get(interaction.guild.text_channels, name=search_name)

            if not channel or not isinstance(channel, discord.TextChannel):
                await interaction.response.send_message(
                    embed=create_error_embed(f"Channel '{channel_str}' not found or is not a text channel."),
                    ephemeral=True
                )
                return

            # Validate frequency
            frequency = self.frequency_input.value.strip().lower()
            if frequency not in ['weekly', 'monthly', 'both']:
                await interaction.response.send_message(
                    embed=create_error_embed("Frequency must be 'weekly', 'monthly', or 'both'."),
                    ephemeral=True
                )
                return

            # Parse report types
            report_types_str = self.report_types_input.value.strip().lower()
            report_types = [t.strip() for t in report_types_str.split(',') if t.strip()]
            valid_types = ['activity', 'members', 'departures']
            report_types = [t for t in report_types if t in valid_types]

            if not report_types:
                await interaction.response.send_message(
                    embed=create_error_embed("At least one valid report type required: activity, members, departures"),
                    ephemeral=True
                )
                return

            # Parse day settings
            day_weekly = 0  # Default Monday
            day_monthly = 1  # Default 1st of month

            if self.day_input.value.strip():
                try:
                    day = int(self.day_input.value.strip())
                    if frequency == 'weekly':
                        if 0 <= day <= 6:
                            day_weekly = day
                        else:
                            raise ValueError("Weekly day must be 0-6")
                    elif frequency == 'monthly':
                        if 1 <= day <= 28:
                            day_monthly = day
                        else:
                            raise ValueError("Monthly day must be 1-28")
                    else:  # both
                        # Use same value for both, validate range
                        if 0 <= day <= 6:
                            day_weekly = day
                        if 1 <= day <= 28:
                            day_monthly = day if day > 0 else 1
                except ValueError as e:
                    await interaction.response.send_message(
                        embed=create_error_embed(f"Invalid day: {str(e)}"),
                        ephemeral=True
                    )
                    return

            # Save configuration
            success = self.db.set_report_config(
                self.guild_id, 
                channel.id, 
                frequency, 
                report_types,
                day_weekly,
                day_monthly,
                interaction.guild.name
            )

            if success:
                day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                schedule_info = ""
                if frequency == 'weekly':
                    schedule_info = f"\n• Sends every **{day_names[day_weekly]}**"
                elif frequency == 'monthly':
                    schedule_info = f"\n• Sends on day **{day_monthly}** of each month"
                else:  # both
                    schedule_info = f"\n• Weekly: every **{day_names[day_weekly]}**\n• Monthly: day **{day_monthly}** of each month"

                await interaction.response.send_message(
                    embed=create_success_embed(
                        f"✅ Scheduled reports configured!\n\n"
                        f"**Channel:** {channel.mention}\n"
                        f"**Frequency:** {frequency.title()}\n"
                        f"**Report Types:** {', '.join(report_types)}"
                        f"{schedule_info}"
                    ),
                    ephemeral=True
                )
                logger.info(f"Reports configured for guild {self.guild_id}: {frequency} to {channel.name}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed("Failed to save report configuration."),
                    ephemeral=True
                )

        except Exception as e:
            await interaction.response.send_message(
                embed=create_error_embed(f"Error configuring reports: {str(e)}"),
                ephemeral=True
            )
            logger.error(f"Failed to configure reports for guild {self.guild_id}: {e}")


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

"""Channel and timing configuration modals."""

import discord
import logging
import pytz

from database import DatabaseManager
from bot.utils import create_error_embed, create_success_embed
from bot.locale import t, guild_language, weekday_name

logger = logging.getLogger(__name__)


class RetentionDaysModal(discord.ui.Modal):
    """Modal for setting message activity retention period."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        guild_config = db.get_guild_config(guild_id)
        self.lang = guild_language(guild_config)
        lang = self.lang
        super().__init__(title=t("channel_config.retention.modal_title", lang))

        self.days_input = discord.ui.TextInput(
            label=t("channel_config.retention.input_label", lang),
            placeholder=t("channel_config.retention.input_placeholder", lang),
            required=True,
            max_length=4
        )
        self.add_item(self.days_input)

        # Pre-fill with current value
        if guild_config and guild_config.get('message_retention_days'):
            self.days_input.default = str(guild_config['message_retention_days'])

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
        try:
            days = int(self.days_input.value.strip())

            if days < 30:
                await interaction.response.send_message(
                    embed=create_error_embed(t("channel_config.retention.min", lang), lang),
                    ephemeral=True
                )
                return

            if days > 3650:  # 10 years
                await interaction.response.send_message(
                    embed=create_error_embed(t("channel_config.retention.max", lang), lang),
                    ephemeral=True
                )
                return

            # Update database
            self.db.set_message_retention_days(self.guild_id, days, interaction.guild.name)

            await interaction.response.send_message(
                embed=create_success_embed(t("channel_config.retention.set", lang, days=days), lang),
                ephemeral=True
            )
            logger.info(f"Message retention set to {days} days for guild {self.guild_id}")

        except ValueError:
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.retention.invalid", lang), lang),
                ephemeral=True
            )


class TimezoneModal(discord.ui.Modal):
    """Modal for setting the server timezone."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        guild_config = db.get_guild_config(guild_id)
        self.lang = guild_language(guild_config)
        lang = self.lang
        super().__init__(title=t("channel_config.timezone.modal_title", lang))

        self.timezone_input = discord.ui.TextInput(
            label=t("channel_config.timezone.input_label", lang),
            placeholder=t("channel_config.timezone.input_placeholder", lang),
            required=True,
            max_length=50
        )
        self.add_item(self.timezone_input)

        # Pre-fill with current value
        if guild_config and guild_config.get('timezone'):
            self.timezone_input.default = guild_config['timezone']

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
        try:
            timezone_str = self.timezone_input.value.strip()

            # Validate timezone
            if timezone_str not in pytz.all_timezones:
                # Try to find close matches
                timezone_lower = timezone_str.lower()
                suggestions = [tz for tz in pytz.all_timezones if timezone_lower in tz.lower()][:5]

                error_msg = t("channel_config.timezone.invalid", lang, tz=timezone_str)
                if suggestions:
                    suggestions_str = "\n".join(t("channel_config.timezone.suggestion_line", lang, tz=tz) for tz in suggestions)
                    error_msg += t("channel_config.timezone.did_you_mean", lang, suggestions=suggestions_str)
                else:
                    error_msg += t("channel_config.timezone.no_suggestions", lang)

                await interaction.response.send_message(
                    embed=create_error_embed(error_msg, lang),
                    ephemeral=True
                )
                return

            # Update database
            self.db.set_timezone(self.guild_id, timezone_str, interaction.guild.name)

            await interaction.response.send_message(
                embed=create_success_embed(t("channel_config.timezone.set", lang, tz=timezone_str), lang),
                ephemeral=True
            )
            logger.info(f"Timezone set to {timezone_str} for guild {self.guild_id}")

        except Exception as e:
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.timezone.error", lang, error=str(e)), lang),
                ephemeral=True
            )
            logger.error(f"Failed to set timezone for guild {self.guild_id}: {e}")


class ReportsConfigModal(discord.ui.Modal):
    """Modal for configuring scheduled reports."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        guild_config = db.get_guild_config(guild_id)
        self.lang = guild_language(guild_config)
        lang = self.lang
        super().__init__(title=t("channel_config.reports.modal_title", lang))

        self.channel_input = discord.ui.TextInput(
            label=t("channel_config.reports.channel_label", lang),
            placeholder=t("channel_config.reports.channel_placeholder", lang),
            required=True,
            max_length=100
        )
        self.frequency_input = discord.ui.TextInput(
            label=t("channel_config.reports.frequency_label", lang),
            placeholder=t("channel_config.reports.frequency_placeholder", lang),
            required=True,
            max_length=10
        )
        self.report_types_input = discord.ui.TextInput(
            label=t("channel_config.reports.types_label", lang),
            placeholder=t("channel_config.reports.types_placeholder", lang),
            required=True,
            max_length=50,
            default="activity,members,departures"
        )
        self.days_input = discord.ui.TextInput(
            label=t("channel_config.reports.days_label", lang),
            placeholder=t("channel_config.reports.days_placeholder", lang),
            required=False,
            max_length=10
        )
        self.hour_input = discord.ui.TextInput(
            label=t("channel_config.reports.hour_label", lang),
            placeholder=t("channel_config.reports.hour_placeholder", lang),
            required=False,
            default="9",
            max_length=2
        )
        for item in (self.channel_input, self.frequency_input, self.report_types_input, self.days_input, self.hour_input):
            self.add_item(item)

        # Pre-fill with current values if configured
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
            
            # Pre-fill schedule fields
            day_weekly = guild_config.get('report_day_weekly', 0)
            day_monthly = guild_config.get('report_day_monthly', 1)
            time_hour = guild_config.get('report_time_hour', 9)
            # Convert internal 0-6 to user-friendly 1-7
            self.days_input.default = f"{day_weekly + 1},{day_monthly}"
            self.hour_input.default = str(time_hour)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
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
                    embed=create_error_embed(t("channel_config.reports.channel_not_found", lang, channel=channel_str), lang),
                    ephemeral=True
                )
                return

            # Validate frequency
            frequency = self.frequency_input.value.strip().lower()
            if frequency not in ['weekly', 'monthly', 'both']:
                await interaction.response.send_message(
                    embed=create_error_embed(t("channel_config.reports.invalid_frequency", lang), lang),
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
                    embed=create_error_embed(t("channel_config.reports.invalid_types", lang), lang),
                    ephemeral=True
                )
                return

            # Parse schedule settings
            day_weekly = 0  # Default Monday
            day_monthly = 1  # Default 1st of month
            time_hour = 9  # Default 9 AM

            # Parse days input (format: weekly,monthly or just one value)
            days_str = self.days_input.value.strip()
            if days_str:
                parts = [p.strip() for p in days_str.split(',')]
                
                # Parse weekly day (first value)
                if len(parts) >= 1 and parts[0]:
                    try:
                        day_weekly = int(parts[0])
                        if not 1 <= day_weekly <= 7:
                            await interaction.response.send_message(
                                embed=create_error_embed(t("channel_config.reports.weekly_day_range", lang), lang),
                                ephemeral=True
                            )
                            return
                        # Convert user input (1-7) to internal format (0-6)
                        day_weekly = day_weekly - 1
                    except ValueError:
                        await interaction.response.send_message(
                            embed=create_error_embed(t("channel_config.reports.weekly_day_nan", lang), lang),
                            ephemeral=True
                        )
                        return

                # Parse monthly day (second value)
                if len(parts) >= 2 and parts[1]:
                    try:
                        day_monthly = int(parts[1])
                        if not 1 <= day_monthly <= 28:
                            await interaction.response.send_message(
                                embed=create_error_embed(t("channel_config.reports.monthly_day_range", lang), lang),
                                ephemeral=True
                            )
                            return
                    except ValueError:
                        await interaction.response.send_message(
                            embed=create_error_embed(t("channel_config.reports.monthly_day_nan", lang), lang),
                            ephemeral=True
                        )
                        return

            # Parse hour
            hour_str = self.hour_input.value.strip()
            if hour_str:
                try:
                    time_hour = int(hour_str)
                    if not 0 <= time_hour <= 23:
                        await interaction.response.send_message(
                            embed=create_error_embed(t("channel_config.reports.hour_range", lang), lang),
                            ephemeral=True
                        )
                        return
                except ValueError:
                    await interaction.response.send_message(
                        embed=create_error_embed(t("channel_config.reports.hour_nan", lang), lang),
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
                time_hour,
                interaction.guild.name
            )

            if success:
                # Get timezone for display
                guild_config = self.db.get_guild_config(self.guild_id)
                guild_tz_str = guild_config.get('timezone', 'UTC') if guild_config else 'UTC'
                
                day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                schedule_info = ""
                if frequency == 'weekly':
                    schedule_info = t("channel_config.reports.schedule_weekly", lang, day=weekday_name(day_names[day_weekly], lang), daynum=day_weekly + 1, hour=time_hour, tz=guild_tz_str)
                elif frequency == 'monthly':
                    schedule_info = t("channel_config.reports.schedule_monthly", lang, day=day_monthly, hour=time_hour, tz=guild_tz_str)
                else:  # both
                    schedule_info = t("channel_config.reports.schedule_both", lang, day=weekday_name(day_names[day_weekly], lang), daynum=day_weekly + 1, monthday=day_monthly, hour=time_hour, tz=guild_tz_str)

                await interaction.response.send_message(
                    embed=create_success_embed(
                        t("channel_config.reports.success", lang,
                          channel=channel.mention,
                          frequency=frequency.title(),
                          types=', '.join(report_types),
                          schedule=schedule_info),
                        lang
                    ),
                    ephemeral=True
                )
                logger.info(f"Reports configured for guild {self.guild_id}: {frequency} to {channel.name} at {time_hour:02d}:00 {guild_tz_str}")
            else:
                await interaction.response.send_message(
                    embed=create_error_embed(t("channel_config.reports.save_failed", lang), lang),
                    ephemeral=True
                )

        except Exception as e:
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.reports.error", lang, error=str(e)), lang),
                ephemeral=True
            )
            logger.error(f"Failed to configure reports for guild {self.guild_id}: {e}")


class ChannelModal(discord.ui.Modal):
    """Modal for setting the notification channel."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        self.lang = guild_language(db.get_guild_config(guild_id))
        super().__init__(title=t("channel_config.channel.modal_title", self.lang))

        self.channel_input = discord.ui.TextInput(
            label=t("channel_config.channel.input_label", self.lang),
            placeholder=t("channel_config.channel.input_placeholder", self.lang),
            required=True,
            max_length=100
        )
        self.add_item(self.channel_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
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
                embed=create_error_embed(t("channel_config.channel.not_found", lang, channel=channel_str), lang),
                ephemeral=True
            )
            return

        # Validate channel type - must be a text channel
        if not isinstance(channel, discord.TextChannel):
            channel_type = type(channel).__name__
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.channel.invalid_type", lang, channel=channel.name, type=channel_type), lang),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_notification_channel(self.guild_id, channel.id, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(t("channel_config.channel.set", lang, channel=channel.mention), lang),
                ephemeral=True
            )
            logger.info(f"Notification channel set to {channel.name} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.channel.update_failed", lang), lang),
                ephemeral=True
            )


class InactiveDaysModal(discord.ui.Modal):
    """Modal for setting the inactive days threshold."""

    def __init__(self, db: DatabaseManager, guild_id: int):
        """
        Initialize modal.

        Args:
            db: Database manager
            guild_id: Discord guild ID
        """
        self.db = db
        self.guild_id = guild_id
        self.lang = guild_language(db.get_guild_config(guild_id))
        super().__init__(title=t("channel_config.inactive_days.modal_title", self.lang))

        self.days_input = discord.ui.TextInput(
            label=t("channel_config.inactive_days.input_label", self.lang),
            placeholder=t("channel_config.inactive_days.input_placeholder", self.lang),
            required=True,
            max_length=3
        )
        self.add_item(self.days_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        lang = self.lang
        days_str = self.days_input.value.strip()

        try:
            days = int(days_str)
            if days < 1 or days > 365:
                raise ValueError("out_of_range")
        except ValueError as e:
            if str(e) == "out_of_range":
                error_msg = t("channel_config.inactive_days.range", lang)
            else:
                error_msg = t("channel_config.inactive_days.invalid", lang)
            await interaction.response.send_message(
                embed=create_error_embed(error_msg, lang),
                ephemeral=True
            )
            return

        # Update database
        if self.db.set_inactive_days(self.guild_id, days, interaction.guild.name):
            await interaction.response.send_message(
                embed=create_success_embed(t("channel_config.inactive_days.set", lang, days=days), lang),
                ephemeral=True
            )
            logger.info(f"Inactive days threshold set to {days} in guild {interaction.guild.name}")
        else:
            await interaction.response.send_message(
                embed=create_error_embed(t("channel_config.inactive_days.update_failed", lang), lang),
                ephemeral=True
            )

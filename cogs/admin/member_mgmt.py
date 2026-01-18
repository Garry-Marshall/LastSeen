"""Member database synchronization functionality."""

import discord
import logging
from datetime import datetime, timezone

from database import DatabaseManager
from bot.utils import get_member_roles, create_success_embed, create_error_embed

logger = logging.getLogger(__name__)


async def update_all_members(
    interaction: discord.Interaction,
    db: DatabaseManager
):
    """
    Enumerate and update all guild members in the database.

    This function:
    - Scans all members in the guild
    - Skips bots
    - Adds new members to database
    - Updates existing members (username, nickname, roles)
    - Reports count of added/updated members

    Args:
        interaction: Discord interaction (must be deferred)
        db: Database manager
    """
    guild = interaction.guild
    added_count = 0
    updated_count = 0

    try:
        # First, ensure guild name is correct
        db.update_guild_name(guild.id, guild.name)

        for member in guild.members:
            if member.bot:
                continue

            roles = get_member_roles(member)
            join_date = int(member.joined_at.timestamp()) if member.joined_at else int(datetime.now(timezone.utc).timestamp())
            nickname = member.display_name if member.display_name != str(member) else None

            if db.member_exists(guild.id, member.id):
                # Update existing member
                db.update_member_username(guild.id, member.id, str(member))
                db.update_member_nickname(guild.id, member.id, nickname)
                db.update_member_roles(guild.id, member.id, roles)
                updated_count += 1
            else:
                # Add new member
                db.add_member(
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

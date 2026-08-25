"""Push guild and user counts to the discordbotlist.com listing.

Only active when DISCORDBOTLIST_TOKEN is set in the environment. Called on
guild join/leave so the listing's counts stay current.
"""

import logging
import aiohttp

logger = logging.getLogger(__name__)

# Bot id is filled in per-request from bot.user.id.
STATS_URL = "https://discordbotlist.com/api/v1/bots/{bot_id}/stats"


async def update_discordbotlist_metrics(bot):
    """POST the current guild and user count to discordbotlist.com.

    No-op if DISCORDBOTLIST_TOKEN is unset. Failures are logged and swallowed
    so a listing outage never disrupts guild event handling.
    """
    token = bot.config.discordbotlist_token
    if not token:
        return

    # Site vanity slug (e.g. "lastseen-2591"); the numeric Discord ID is rejected.
    bot_id = bot.config.discordbotlist_bot_id or bot.user.id

    payload = {
        "guilds": len(bot.guilds),
        "users": sum(g.member_count or 0 for g in bot.guilds),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                STATS_URL.format(bot_id=bot_id),
                headers={"Authorization": token},
                json=payload,
            ) as resp:
                if 200 <= resp.status < 300:
                    logger.info(
                        f"Updated DiscordBotList metrics: {payload['guilds']} guilds, "
                        f"{payload['users']} users"
                    )
                else:
                    body = await resp.text()
                    logger.warning(f"DiscordBotList metrics update failed ({resp.status}): {body}")
    except Exception as e:
        logger.error(f"Error updating DiscordBotList metrics: {e}", exc_info=True)

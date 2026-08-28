"""Push server and shard counts to the voidbots.net listing.

Only active when VOIDBOTS_TOKEN is set in the environment. Called on guild
join/leave so the listing's server count stays current.
"""

import logging
import aiohttp

logger = logging.getLogger(__name__)

# Bot id is filled in per-request from bot.user.id.
STATS_URL = "https://api.voidbots.net/bot/stats/{bot_id}"


async def update_voidbots_metrics(bot):
    """POST the current server and shard count to voidbots.net.

    No-op if VOIDBOTS_TOKEN is unset. Failures are logged and swallowed so a
    listing outage never disrupts guild event handling.
    """
    token = bot.config.voidbots_token
    if not token:
        return

    payload = {
        "server_count": len(bot.guilds),
        "shard_count": bot.shard_count,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                STATS_URL.format(bot_id=bot.user.id),
                headers={"Authorization": token},
                json=payload,
            ) as resp:
                if 200 <= resp.status < 300:
                    logger.info(
                        f"Updated VoidBots metrics: {payload['server_count']} servers, "
                        f"{payload['shard_count']} shard(s)"
                    )
                else:
                    body = await resp.text()
                    logger.warning(f"VoidBots metrics update failed ({resp.status}): {body}")
    except Exception as e:
        logger.error(f"Error updating VoidBots metrics: {e}", exc_info=True)

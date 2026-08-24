"""Push server and shard counts to the TOP.GG listing.

Only active when TOPGG_TOKEN is set in the environment. Called on guild
join/leave so the listing's server count stays current.
"""

import logging
import aiohttp

logger = logging.getLogger(__name__)

METRICS_URL = "https://top.gg/api/v1/projects/@me/metrics"


async def update_topgg_metrics(bot):
    """PATCH the current server and shard count to TOP.GG.

    No-op if TOPGG_TOKEN is unset. Failures are logged and swallowed so a
    listing outage never disrupts guild event handling.
    """
    token = bot.config.topgg_token
    if not token:
        return

    payload = {
        "server_count": len(bot.guilds),
        "shard_count": bot.shard_count,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                METRICS_URL,
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            ) as resp:
                if 200 <= resp.status < 300:
                    # TOP.GG returns 204 No Content on success.
                    logger.info(
                        f"Updated TOP.GG metrics: {payload['server_count']} servers, "
                        f"{payload['shard_count']} shard(s)"
                    )
                else:
                    body = await resp.text()
                    logger.warning(f"TOP.GG metrics update failed ({resp.status}): {body}")
    except Exception as e:
        logger.error(f"Error updating TOP.GG metrics: {e}", exc_info=True)

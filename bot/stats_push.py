"""Push aggregate server/user counts to a LAN webserver via scp.

Only active when STATS_PUSH_ENABLED is set in the environment. A small
stats.json (server count, user count, UTC timestamp) is written locally and
copied to the webserver on a timer. Disabled by default so a dev instance
never overwrites the production listing's stats file.

The scp destination and SSH key are read from .env (STATS_SCP_DESTINATION,
STATS_SSH_KEY) so they never reach the git repo.
"""

import asyncio
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Local staging file (not sensitive; the remote target and key live in .env).
STATS_LOCAL = Path("stats.json")


async def push_stats(bot):
    """Write stats.json and scp it to the webserver.

    No-op unless STATS_PUSH_ENABLED is set. scp runs in a worker thread so the
    subprocess never blocks the event loop; failures are logged and swallowed
    so a webserver or network outage never disrupts the bot.
    """
    if not bot.config.stats_push_enabled:
        return

    destination = bot.config.stats_scp_destination
    ssh_key = bot.config.stats_ssh_key
    if not destination or not ssh_key:
        logger.warning(
            "Stats push enabled but STATS_SCP_DESTINATION or STATS_SSH_KEY is unset; skipping"
        )
        return

    # Use the same DB-backed counts as /about (tracked, active, non-bot members)
    # so the public number matches what the bot reports in-app. Runs in a worker
    # thread so the cross-guild full scan never blocks the event loop. The DB
    # layer caches for 5 min, but at a 10-min cadence the cache is usually cold
    # here, so most pushes do a fresh scan — cheap and infrequent enough not to
    # matter.
    stats = await asyncio.to_thread(bot.db.get_bot_statistics)
    data = {
        "servers": stats["total_guilds"],
        "users": stats["total_users"],
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    try:
        STATS_LOCAL.write_text(json.dumps(data))
    except Exception as e:
        logger.error(f"Failed to write {STATS_LOCAL}: {e}", exc_info=True)
        return

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            ["scp", "-i", ssh_key, str(STATS_LOCAL), destination],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            logger.info(
                f"Pushed stats to webserver: {data['servers']} servers, "
                f"{data['users']} users"
            )
        else:
            logger.warning(
                f"stats scp failed (exit {result.returncode}): {result.stderr.strip()}"
            )
    except Exception as e:
        logger.error(f"Error pushing stats to webserver: {e}", exc_info=True)

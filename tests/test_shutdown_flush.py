"""Shutdown writes every buffered activity entry before the pool closes (issue 22)."""
import asyncio, os, sys, time, tempfile, threading, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from database import DatabaseManager
from bot.client import LastSeenBot
from cogs.tracking import TrackingCog

# This bot never connected, so discord.py's sharded gateway close has nothing
# to close (and would crash on its missing event queue). Stub just that step;
# Bot.close (cog unload) and LastSeenBot.close (pool close) stay real.
async def _no_gateway_close(self):
    pass
discord.AutoShardedClient.close = _no_gateway_close

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

G = 1
now = datetime.now(timezone.utc)
TODAY = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
HOUR = int(datetime(now.year, now.month, now.day, now.hour, tzinfo=timezone.utc).timestamp())


async def main():
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "s.db"), pool_size=4)
    db.add_guild(G, "G")
    for uid in range(1, 7):
        db.add_member(G, uid, f"u{uid}", None, 0, [])

    # Slow writes + overlap tracking
    active = {'now': 0, 'max': 0}; lock = threading.Lock()
    real = db.increment_message_activity
    def slow(*a, **kw):
        with lock: active['now'] += 1; active['max'] = max(active['max'], active['now'])
        try:
            time.sleep(0.05); return real(*a, **kw)
        finally:
            with lock: active['now'] -= 1
    db.increment_message_activity = slow

    bot = LastSeenBot(command_prefix='!', intents=discord.Intents.none(), help_command=None)
    bot.db = db
    bot.config = SimpleNamespace(stats_push_enabled=False, default_inactive_days=10)
    bot.opted_out_users = set()
    cog = TrackingCog(bot, db, bot.config)
    await bot.add_cog(cog)

    # A periodic flush is mid-write when shutdown starts...
    cog.daily_activity_buffer[(G, 1, TODAY)] += 5
    cog.daily_activity_buffer[(G, 2, TODAY)] += 5
    running = asyncio.create_task(cog.flush_activity_buffer())
    await asyncio.sleep(0.02)
    # ...while more lands in every buffer the old shutdown flush ignored or raced
    cog.daily_activity_buffer[(G, 3, TODAY)] += 7
    cog.hourly_activity_buffer[(G, 3, HOUR, now.hour)] += 7
    cog.failed_daily_writes[(G, 4, TODAY)] += 11          # a failed write awaiting retry
    cog.return_buffer.append((G, 5, 40 * 86400, int(now.timestamp())))   # a returning member

    await bot.close()          # what Ctrl+C / restart runs: unload cogs, disconnect, close pool
    await asyncio.gather(running, return_exceptions=True)

    check2 = DatabaseManager(db.db_file, pool_size=1)   # fresh manager: the bot's pool is closed now
    with check2.get_connection() as c:
        counts = dict(c.execute("SELECT user_id, SUM(message_count) FROM message_activity GROUP BY user_id").fetchall())
        hourly = c.execute("SELECT SUM(message_count) FROM message_activity_hourly").fetchone()[0]
        returns = c.execute("SELECT COUNT(*) FROM member_returns").fetchone()[0]
    check(counts == {1: 5, 2: 5, 3: 7, 4: 11}, f"all daily counts written, none doubled: {counts}")
    check(hourly == 7, f"hourly buffer written ({hourly})")
    check(returns == 1, f"returning-member record written ({returns})")
    check(active['max'] == 1, f"final flush waited for the running one (max concurrent writers {active['max']})")
    check(check2.get_bot_state('heartbeat') is not None, "final heartbeat written")
    check2.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

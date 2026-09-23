"""Message-activity flushes never overlap (issue 21)."""
import asyncio, os, sys, time, tempfile, threading, logging
from collections import defaultdict
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from database import DatabaseManager
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

G = 1
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "f.db"), pool_size=8)
db.add_guild(G, "G")
for uid in range(1, 21):
    db.add_member(G, uid, f"u{uid}", None, 0, [])

# Slow each write down so a second flush has plenty of time to overlap.
active = {'now': 0, 'max': 0}
lock = threading.Lock()
real_increment = db.increment_message_activity
def slow_increment(*a, **kw):
    with lock:
        active['now'] += 1; active['max'] = max(active['max'], active['now'])
    try:
        time.sleep(0.02)
        return real_increment(*a, **kw)
    finally:
        with lock:
            active['now'] -= 1
db.increment_message_activity = slow_increment

cog = TrackingCog.__new__(TrackingCog)
cog.db = db
cog.bot = SimpleNamespace(opted_out_users=set())
cog.daily_activity_buffer = defaultdict(int)
cog.hourly_activity_buffer = defaultdict(int)
cog.failed_daily_writes = defaultdict(int)
cog.failed_hourly_writes = defaultdict(int)
cog.return_buffer = []
cog.MAX_BUFFER_SIZE = 10
cog._flush_lock = asyncio.Lock
cog._down_shards = set()
guild = SimpleNamespace(id=G, name="G")

def message(uid):
    return SimpleNamespace(author=SimpleNamespace(id=uid, bot=False), guild=guild)

async def main():
    cog._flush_lock = asyncio.Lock()
    sent = 0
    # Fill the buffer to the limit: the 10th message forces a (slow) flush
    tasks = []
    for uid in range(1, 11):
        tasks.append(asyncio.create_task(TrackingCog.on_message(cog, message(uid)))); sent += 1
    await asyncio.sleep(0.05)   # that flush is now running in its thread
    # More messages past the limit, plus a periodic tick, while it runs
    for uid in list(range(11, 21)) + list(range(1, 11)):
        tasks.append(asyncio.create_task(TrackingCog.on_message(cog, message(uid)))); sent += 1
    tasks.append(asyncio.create_task(TrackingCog.flush_activity_buffer.coro(cog)))
    await asyncio.gather(*tasks)
    await TrackingCog.flush_activity_buffer.coro(cog)   # drain whatever is left

    check(active['max'] == 1, f"flush threads writing at the same time: max {active['max']} (must be 1)")
    with db.get_connection() as c:
        total = c.execute("SELECT SUM(message_count) FROM message_activity").fetchone()[0]
        days = c.execute("SELECT MAX(active_days), SUM(total_messages) FROM member_activity_summary").fetchone()
    check(total == sent, f"all {sent} messages stored exactly once ({total})")
    check(days[0] == 1 and days[1] == sent, f"journey summary: 1 active day per member, {days[1]} messages")

asyncio.run(main())
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

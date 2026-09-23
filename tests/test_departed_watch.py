"""offline_for user watch must not fire for a member who left (issue 14)."""
import asyncio, os, sys, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales
from database import DatabaseManager
from cogs.watch import WatchCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
D, G, CHANNEL, TARGET = 86400, 1, 555, 20


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "d.db"), pool_size=2)
    db.add_guild(G, "G")
    db.add_member(G, TARGET, "target", None, NOW - 300 * D, [])

    sent = []
    channel = SimpleNamespace(id=CHANNEL, name="alerts", permissions_for=lambda me: SimpleNamespace(send_messages=True))
    async def send(embed=None): sent.append(embed)
    channel.send = send
    guild = SimpleNamespace(id=G, name="G", me=None, get_channel_or_thread=lambda cid: channel if cid == CHANNEL else None)
    cog = WatchCog.__new__(WatchCog)
    cog.db, cog.bot = db, SimpleNamespace(no_watch_users=set(), opted_out_users=set())

    db.add_watch(G, 'user', TARGET, 'offline_for', 7 * D, CHANNEL, 99)
    watch = lambda: db.get_guild_watches(G)[0]

    # Left the server 10 days ago (on_member_remove sets last_seen = leave time)
    with db.get_connection() as c:
        c.execute("UPDATE members SET is_active = 0, last_seen = ?, left_date = ? WHERE guild_id = ? AND user_id = ?",
                  (NOW - 10 * D, NOW - 10 * D, G, TARGET))
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(not sent and watch()['state'] == 'armed', "departed target: no alert, watch kept armed (fired before)")

    # Rejoins while online -> watch applies again once they go offline long enough
    db.rejoin_member(G, TARGET, "target", None, NOW - D, [], True)
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(not sent, "rejoined and online: no alert")
    db.update_last_seen(G, TARGET, NOW - 8 * D)
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(len(sent) == 1 and watch()['state'] == 'triggered', "rejoined, then offline 8 days: alert fires")

    # Regression: a current member offline past the threshold still fires
    db.add_member(G, 21, "other", None, NOW - 300 * D, [])
    db.update_last_seen(G, 21, NOW - 9 * D)
    db.add_watch(G, 'user', 21, 'offline_for', 7 * D, CHANNEL, 99)
    w21 = [w for w in db.get_guild_watches(G) if w['target_id'] == 21][0]
    await cog._sweep_user_watch(guild, w21, NOW)
    check(len(sent) == 2, "current member offline 9 days: alert fires as before")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

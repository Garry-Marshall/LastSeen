"""Verify channel watches are removed after repeated failed deliveries."""
import asyncio
import os
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.watch import WatchCog, MAX_DELIVERY_FAILURES

G, ADMIN, TARGET, CHAN = 1, 10, 20, 555
failures = []


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


class FakeChannel:
    def __init__(self):
        self.id, self.name = CHAN, "alerts"
        self.can_send, self.raise_on_send, self.sent = True, None, []
    def permissions_for(self, me): return SimpleNamespace(send_messages=self.can_send)
    async def send(self, embed=None):
        if self.raise_on_send:
            raise self.raise_on_send
        self.sent.append(embed)


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "w.db"), pool_size=2)
    db.add_guild(G, "Guild")

    channel = FakeChannel()
    guild = SimpleNamespace(id=G, name="Guild", me=None, unavailable=False, channel=channel)
    guild.get_channel_or_thread = lambda cid: guild.channel if cid == CHAN else None
    guild.get_member = lambda uid: None
    bot = SimpleNamespace(watch_guild_ids=set())
    cog = SimpleNamespace(db=db, bot=bot)
    for name in ('_fire_alert', '_record_delivery_failure', '_refresh_watch_guilds', '_dm_recipient'):
        attr = getattr(WatchCog, name)
        setattr(cog, name, attr if isinstance(WatchCog.__dict__[name], staticmethod) else attr.__get__(cog))

    db.add_watch(G, 'user', TARGET, 'online_return', None, CHAN, ADMIN)
    cog._refresh_watch_guilds()
    watch = lambda: (db.get_guild_watches(G) or [None])[0]
    fire = lambda: cog._fire_alert(guild, watch(), "<@20>")

    # 1. No send permission counts
    channel.can_send = False
    await fire()
    check(watch()['failed_deliveries'] == 1, "missing send permission counts a failure")

    # 2. Forbidden / NotFound from the API count; transient errors don't
    channel.can_send = True
    resp = SimpleNamespace(status=403, reason="Forbidden")
    channel.raise_on_send = discord.Forbidden(resp, "Missing Access")
    await fire()
    check(watch()['failed_deliveries'] == 2, "Forbidden on send counts a failure")
    channel.raise_on_send = RuntimeError("503")
    await fire()
    check(watch()['failed_deliveries'] == 2, "transient send error not counted")

    # 3. Successful delivery resets the count
    channel.raise_on_send = None
    await fire()
    check(watch()['failed_deliveries'] == 0 and len(channel.sent) == 1, "successful send resets the count")

    # 4. Unavailable guild (outage) isn't counted
    guild.channel, guild.unavailable = None, True
    await fire()
    check(watch()['failed_deliveries'] == 0, "unavailable guild not counted")
    guild.unavailable = False

    # 5. Reconfiguring the watch resets the count
    await fire()
    check(watch()['failed_deliveries'] == 1, "deleted channel counts a failure")
    db.add_watch(G, 'user', TARGET, 'online_return', None, CHAN, ADMIN)
    check(watch()['failed_deliveries'] == 0, "re-adding the watch resets the count")

    # 6. Threshold reached -> watch removed, guild dropped from the index
    for _ in range(MAX_DELIVERY_FAILURES - 1):
        await fire()
    check(watch() is not None and watch()['failed_deliveries'] == MAX_DELIVERY_FAILURES - 1,
          f"still kept after {MAX_DELIVERY_FAILURES - 1} failures")
    await fire()
    check(watch() is None, f"removed after {MAX_DELIVERY_FAILURES} consecutive failures")
    check(G not in bot.watch_guild_ids, "guild removed from the watch index")

    # 7. DM watches are not affected by the channel counter
    db.add_watch(G, 'user', TARGET, 'offline_for', 86400, None, ADMIN, deliver_dm=True)
    await fire()  # recipient not cached -> paused, not a channel failure
    check(watch()['failed_deliveries'] == 0, "paused DM watch not counted")

    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED")
sys.exit(1 if failures else 0)

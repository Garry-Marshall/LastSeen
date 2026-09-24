"""Join/leave/update of one member are written in event order, and a join
already stored by the presence path isn't treated as a rejoin."""
import asyncio, os, sys, tempfile, logging, threading, time
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from database import DatabaseManager
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
G = 1


class FakeRole:
    def __init__(self, name): self.name = name


class FakeMember:
    def __init__(self, uid, guild, roles=('@everyone',), joined=NOW - 60, online=True):
        self.id, self.guild, self.bot, self.nick = uid, guild, False, None
        self.name = self.display_name = f"user{uid}"
        self.roles = [FakeRole(r) for r in roles]
        self.top_role = self.roles[-1]
        self.joined_at = datetime.fromtimestamp(joined, tz=timezone.utc)
        self.status = discord.Status.online if online else discord.Status.offline
        self.display_avatar = SimpleNamespace(url="https://x")
    def __str__(self): return self.name


class SlowJoinDB:
    """Wraps DatabaseManager: add_member for the users in `slow` waits (in its
    worker thread) until `release` is set, or 0.3 s by default — a join
    stuck behind a busy database."""
    def __init__(self, db):
        self._db, self.slow, self.release, self.rejoins = db, set(), threading.Event(), []
    def __getattr__(self, name):
        return getattr(self._db, name)
    def add_member(self, guild_id, user_id, *a, **kw):
        if user_id in self.slow:
            self.release.wait(0.3)
        return self._db.add_member(guild_id, user_id, *a, **kw)
    def rejoin_member(self, *a, **kw):
        self.rejoins.append(a[1])
        return self._db.rejoin_member(*a, **kw)


def dispatch(coro):
    """discord.py runs every event handler as its own task, in arrival order."""
    return asyncio.create_task(coro)


async def main():
    real = DatabaseManager(os.path.join(tempfile.mkdtemp(), "m.db"), pool_size=4)
    real.add_guild(G, "G")
    real.mark_positions_initialized(G)
    real.add_member(G, 1, "user1", None, NOW - 999999, [])
    db = SlowJoinDB(real)
    guild = SimpleNamespace(id=G, name="G")
    cog = TrackingCog.__new__(TrackingCog)
    cog.db, cog.config, cog._member_locks = db, SimpleNamespace(default_inactive_days=10), {}
    cog.bot = SimpleNamespace(opted_out_users=set(), get_channel=lambda cid: None)
    row = lambda uid: real.get_member(G, uid)

    # 1. Instant kick: the leave arrives while the (slow) join is still being written
    db.slow = {10}
    m = FakeMember(10, guild)
    await asyncio.gather(dispatch(TrackingCog.on_member_join(cog, m)),
                         dispatch(TrackingCog.on_member_remove(cog, m)))
    r = row(10)
    check(r and r['is_active'] == 0 and r['left_date'],
          f"join then instant kick: member ends up departed, not a ghost -> active={r and r['is_active']}")

    # 2. Auto-role: role added right after the join, while the join is still being written
    db.slow = {11}
    before = FakeMember(11, guild)
    after = FakeMember(11, guild, roles=('@everyone', 'Verified'))
    await asyncio.gather(dispatch(TrackingCog.on_member_join(cog, before)),
                         dispatch(TrackingCog.on_member_update(cog, before, after)))
    hist = [h['role_name'] for h in real.get_role_history(G, 11)]
    check(row(11)['roles'] == ['Verified'] and hist == ['Verified'],
          f"auto-role right after join: role stored and in role history -> {hist}")

    # 3. Presence update stored the member before the join handler ran
    db.slow = set()
    m = FakeMember(12, guild)
    cog._ensure_member_exists(m)             # what the presence queue does for an unknown member
    stored = row(12)
    await TrackingCog.on_member_join(cog, m)
    check(12 not in db.rejoins, "join already stored by the presence path: not treated as a rejoin")
    check(row(12)['join_date'] == stored['join_date'] and row(12)['join_position'] == stored['join_position'] == 2,
          f"join date and position unchanged -> position {row(12)['join_position']}")

    # 4. Real rejoin after leaving: new join date, departure kept
    m = FakeMember(10, guild, joined=NOW)
    await TrackingCog.on_member_join(cog, m)
    r = row(10)
    check(10 in db.rejoins and r['is_active'] == 1 and r['join_date'] == NOW and r['left_date'],
          "rejoin after a leave: reactivated, new join date, left_date kept")

    # 5. Left while the bot was offline (row still active) and rejoined live: still a rejoin
    m = FakeMember(1, guild, joined=NOW)
    await TrackingCog.on_member_join(cog, m)
    check(1 in db.rejoins and row(1)['join_date'] == NOW, "missed departure + live rejoin: join date updated")

    # 6. Different members don't wait for each other
    db.slow, db.release = {20}, threading.Event()
    slow_join = dispatch(TrackingCog.on_member_join(cog, FakeMember(20, guild)))
    await asyncio.sleep(0.02)
    t0 = time.monotonic()
    await TrackingCog.on_member_join(cog, FakeMember(21, guild))
    check(time.monotonic() - t0 < 0.2 and row(21) and not row(20),
          "another member's join completes while the first is still being written")
    db.release.set()
    await slow_join

    # 7. Presence update right behind a (slow) join: queued only once the join is
    #    stored, so the presence queue never adds the member itself ("joined late")
    cog._presence_queue, cog._presence_dropped = asyncio.Queue(), 0
    late = []
    real_late = cog._calculate_and_set_join_position
    cog._calculate_and_set_join_position = lambda m: late.append(m.id) or real_late(m)
    db.slow, db.release = {30}, threading.Event()
    offline, online = FakeMember(30, guild, online=False), FakeMember(30, guild, online=True)
    join = dispatch(TrackingCog.on_member_join(cog, offline))   # a new member's cache entry has no presence yet
    p1 = dispatch(TrackingCog.on_presence_update(cog, offline, online))
    p2 = dispatch(TrackingCog.on_presence_update(cog, online, offline))
    drain = lambda: [cog._presence_queue.get_nowait() for _ in range(cog._presence_queue.qsize())]
    await asyncio.sleep(0.05)
    early = drain()   # the queue consumer applies whatever arrives, join or not
    await asyncio.to_thread(cog._apply_presence_batch, early)
    check(not early, "presence updates wait while the join is still being written")
    db.release.set()
    await asyncio.gather(join, p1, p2)
    queued = drain()
    check([went_offline for _, went_offline, _ in early + queued] == [False, True],
          "then queued in arrival order (online, offline)")
    await asyncio.to_thread(cog._apply_presence_batch, queued)
    check(not late and 30 not in db.rejoins and row(30)['last_seen'] and row(30)['join_position'],
          f"no 'joined late', no rejoin; presence applied to the stored join -> late={late}")

    # 8. Presence update with nothing pending for that member: queued straight away
    await TrackingCog.on_presence_update(cog, offline, online)
    check(cog._presence_queue.qsize() == 1, "no pending join: presence queued immediately")

    check(cog._member_locks == {}, "no per-member lock entries left behind")
    real.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

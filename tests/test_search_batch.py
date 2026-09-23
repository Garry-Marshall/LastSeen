"""/search: one grouped activity query instead of ~4 per member (issue 19)."""
import asyncio, os, sys, time, random, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

random.seed(5)
D = 86400
now = datetime.now(timezone.utc)
TODAY = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
G, N = 1, 10000


class CountingDB:
    def __init__(self, db): self._db, self.calls = db, {}
    def __getattr__(self, name):
        attr = getattr(self._db, name)
        if not callable(attr): return attr
        def wrapped(*a, **kw):
            self.calls[name] = self.calls.get(name, 0) + 1
            return attr(*a, **kw)
        return wrapped


class FakeMember:
    def __init__(self, uid):
        self.id, self.name, self.display_name, self.bot = uid, f"u{uid}", f"u{uid}", False
        self.status = discord.Status.online if uid % 2 else discord.Status.offline
        self.roles = []


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "s.db"), pool_size=4)
    db.add_guild(G, "Big")
    with db.get_connection() as c:
        c.executemany("INSERT INTO members (guild_id,user_id,username,join_date,last_seen,is_active,roles) VALUES (1,?,?,0,?,?,'[]')",
                      [(u, f"u{u}", TODAY - 5 * D, 0 if u > 9990 else 1) for u in range(1, N + 1)])
        # 40 days of history so the 30-day cutoff, the week and today all matter
        c.executemany("INSERT INTO message_activity (guild_id,user_id,date,message_count) VALUES (1,?,?,?)",
                      [(u, TODAY - d * D, random.randint(1, 9)) for u in range(1, N + 1, 3) for d in range(0, 40) if random.random() < 0.3])

    # ---------- 1. same numbers as the per-member function ----------
    totals = db.get_guild_activity_totals(G, 30)
    sample = random.sample(range(1, N + 1), 400) + [2, 3, 4]   # includes members with no activity
    mismatch = 0
    for u in sample:
        per = db.get_message_activity_period(G, u, 30)
        got = totals.get(u, {'total': 0, 'this_week': 0, 'today': 0})
        mismatch += (per['total'], per['this_week'], per['today']) != (got['total'], got['this_week'], got['today'])
    check(mismatch == 0, f"grouped totals == get_message_activity_period for {len(sample)} members (mismatches: {mismatch})")

    # ---------- 2. speed: old per-member vs new grouped ----------
    t = time.perf_counter()
    for u in range(1, N + 1):
        db.get_message_activity_period(G, u, 30)
    old = time.perf_counter() - t
    t = time.perf_counter(); db.get_guild_activity_totals(G, 30); new = time.perf_counter() - t
    print(f"     {N:,} members: per-member {old:.2f}s ({N * 4:,} queries) vs grouped {new * 1000:.0f} ms (1 query)")
    check(new < old / 20, "grouped query is at least 20x faster")

    # ---------- 3. end-to-end /search: one activity query, filter + rows correct ----------
    counting = CountingDB(db)
    cog = CommandsCog(None, counting, None)
    cached = {u: FakeMember(u) for u in range(1, 9001)}           # 9000 cached, 1000 not (cache miss path)
    guild = SimpleNamespace(id=G, members=list(cached.values()), roles=[], get_role=lambda rid: None)
    sent = []
    async def defer(**kw): pass
    async def send(*a, **kw): sent.append(kw)
    interaction = SimpleNamespace(
        guild_id=G, guild=guild, channel_id=5, channel=None,
        user=SimpleNamespace(id=99, guild_permissions=SimpleNamespace(administrator=True), roles=[]),
        response=SimpleNamespace(defer=defer, send_message=send), followup=SimpleNamespace(send=send))
    await CommandsCog.search.callback(cog, interaction, activity=">40", export="none")
    view = sent[-1].get('view') if sent else None
    results = view.results if view else []
    expected = {u for u, v in totals.items() if v['total'] > 40 and u <= 9990}  # active members only
    check(counting.calls.get('get_guild_activity_totals') == 1 and 'get_message_activity_period' not in counting.calls,
          f"search made 1 grouped activity query, 0 per-member ones -> {counting.calls}")
    got_ids = {r['user_id'] for r in results}
    check(got_ids <= expected and len(got_ids) == min(len(expected), 1000),
          f"activity:>40: {len(got_ids)} results, all expected (of {len(expected)}; /search caps at 1,000)")
    sent.clear()
    await CommandsCog.search.callback(cog, interaction, activity=">80", export="none")
    exp80 = {u for u, v in totals.items() if v['total'] > 80 and u <= 9990}
    got80 = {r['user_id'] for r in sent[-1]['view'].results} if sent and sent[-1].get('view') else set()
    check(0 < len(exp80) < 1000 and got80 == exp80, f"activity:>80 returns exactly the {len(exp80)} expected members")
    check(all(r['activity_30d'] == totals[r['user_id']]['total'] for r in results), "result rows show the right 30-day counts")
    check(any(r['user_id'] > 9000 for r in results) and all(r['activity_30d'] > 40 for r in results if r['user_id'] > 9000),
          "uncached members are now activity-filtered too (they skipped the filter before)")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

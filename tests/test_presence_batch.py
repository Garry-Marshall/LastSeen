"""Batched presence writes keep the old per-event semantics (issue 20)."""
import os, sys, time, sqlite3, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from database import DatabaseManager
from cogs.tracking import TrackingCog, _GuildReconcile

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

G = 1
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "b.db"), pool_size=4)
db.add_guild(G, "G")
guild = SimpleNamespace(id=G, name="G")


class M:
    def __init__(self, uid):
        self.id, self.guild, self.bot, self.name, self.display_name = uid, guild, False, f"u{uid}", f"u{uid}"
        self.joined_at = datetime.fromtimestamp(1_600_000_000, tz=timezone.utc)
        self.roles = [SimpleNamespace(name='@everyone')]
        self.status = discord.Status.online
    def __str__(self): return self.name


for uid in (1, 2, 3):
    db.add_member(G, uid, f"u{uid}", None, 1_600_000_000, [])
db.update_last_seen(G, 2, 0)           # user 2 is online

cog = TrackingCog.__new__(TrackingCog)
cog.db, cog.bot = db, SimpleNamespace(opted_out_users={3})
last_seen = lambda uid: (db.get_member(G, uid) or {}).get('last_seen', 'missing')

m1, m2, m3, m9 = M(1), M(2), M(3), M(9)
res = TrackingCog._apply_presence_batch(cog, [
    (m1, True, 1000),    # offline
    (m1, False, 1100),   # online: previous must be 1000 (from this same batch)
    (m1, True, 1200),    # offline again
    (m2, False, 1300),   # already online: previous 0
    (m3, True, 1400),    # opted out: skipped
    (m9, False, 1500),   # no row yet: created, then applied (previous None)
])
by_user = {(m.id, ts): prev for m, prev, ts in res}
check(last_seen(1) == 1200, f"same member, 3 events in one batch: applied in order (last_seen {last_seen(1)})")
check(by_user.get((1, 1100)) == 1000, "online event sees the offline timestamp written earlier in the batch")
check(by_user.get((2, 1300)) == 0, "already-online member: previous 0 (no return/watch trigger)")
check(last_seen(3) is None and (3, 1400) not in by_user, "opted-out member skipped")
check(last_seen(9) == 0 and (9, 1500) in by_user and by_user[(9, 1500)] is None,
      "member without a row: created, marked online, previous None")
check(len(res) == 3, f"online results only for online events ({len(res)})")

# Reconcile item in the middle keeps queue order:
# event before (online) -> reconcile (snapshot says offline) -> event after (online)
db.update_last_seen(G, 1, 5000)
item = _GuildReconcile(G, "G", {1: False, 2: False, 9: False}, {}, since=7000)
TrackingCog._apply_presence_batch(cog, [(m1, False, 6000), item])
check(last_seen(1) == 7000, f"online event applied before the reconcile that follows it (last_seen {last_seen(1)})")
TrackingCog._apply_presence_batch(cog, [item, (m1, False, 8000)])
check(last_seen(1) == 0, "event queued after a reconcile is applied after it")

# Locked database: the batch fails cleanly after busy_timeout, nothing raised, nothing half-written
db.update_last_seen(G, 2, 111)
blocker = sqlite3.connect(db.db_file, isolation_level=None)
blocker.execute("BEGIN IMMEDIATE")
t = time.perf_counter()
res = TrackingCog._apply_presence_batch(cog, [(m2, False, 9000), (m1, True, 9100)])
waited = time.perf_counter() - t
blocker.execute("ROLLBACK"); blocker.close()
check(res == [] and last_seen(2) == 111, f"locked DB: batch dropped cleanly after {waited:.1f}s, no partial write")
res = TrackingCog._apply_presence_batch(cog, [(m2, False, 9200)])
check(res and res[0][1] == 111 and last_seen(2) == 0, "next batch works normally")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

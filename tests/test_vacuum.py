"""Startup VACUUM: threshold, prod-size timing, WAL reset, failure isolation (issue 6)."""
import os, sys, tempfile, time, sqlite3, logging, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

path = os.path.join(tempfile.mkdtemp(), "v.db")
db = DatabaseManager(path, pool_size=4)
check(db.vacuum_if_fragmented() is False, "fresh database: skipped")

# ~250 MB like prod, then free ~30% (a large guild removed)
db.add_guild(1, "Big"); db.add_guild(2, "Gone")
with db.get_connection() as c:
    for g, n in ((1, 7000), (2, 3000)):
        c.executemany("INSERT INTO members (guild_id,user_id,username,roles) VALUES (?,?,?,'[]')",
                      [(g, u, f"user{u}") for u in range(n)])
    for g, n in ((1, 7000), (2, 3000)):
        c.executemany("INSERT INTO message_activity_hourly (guild_id,user_id,timestamp,hour,message_count) VALUES (?,?,?,0,1)",
                      ((g, u % n, 1700000000 + i * 3600) for i, u in enumerate(range(n * 300))))
db.checkpoint_wal("TRUNCATE")
size = os.path.getsize(path) / 2**20
db.remove_guild_data(2)
check(db.vacuum_if_fragmented is not None, f"built {size:.0f} MB database, removed a guild")

t = time.perf_counter(); ran = db.vacuum_if_fragmented(); dt = time.perf_counter() - t
after = os.path.getsize(path) / 2**20
check(ran and after < size * 0.8, f"VACUUM ran: {size:.0f} MB -> {after:.0f} MB in {dt:.1f}s")
check(os.path.getsize(path + "-wal") < 1 << 20, f"WAL reset after VACUUM ({os.path.getsize(path + '-wal')} bytes)")
check(db.vacuum_if_fragmented() is False, "second run: nothing to reclaim, skipped")

# Pooled connections untouched and still transactional
iso = [c.isolation_level for c in list(db._pool.queue)]
check(all(i == "" for i in iso), f"pooled connections keep transaction mode {iso}")

# Failure: another writer holds the lock -> VACUUM fails cleanly, pool unaffected
db.add_guild(3, "Frag")
with db.get_connection() as c:
    c.executemany("INSERT INTO members (guild_id,user_id,username,roles) VALUES (3,?,?,'[]')", [(u, "x" * 200) for u in range(100000)])
db.remove_guild_data(3)
blocker = sqlite3.connect(path, isolation_level=None)
blocker.execute("BEGIN IMMEDIATE"); blocker.execute("INSERT INTO bot_state VALUES ('x', 1)")
check(db.vacuum_if_fragmented() is False, "locked database: VACUUM fails and returns False")
blocker.execute("ROLLBACK"); blocker.close()
check(all(c.isolation_level == "" for c in list(db._pool.queue)), "pool still transactional after failure")
check(db.set_bot_state('y', 2) and db.get_bot_state('y') == 2, "database usable after failed VACUUM")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

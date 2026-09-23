"""The health check really tests writing (issue 33)."""
import os, sys, stat, sqlite3, tempfile, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

path = os.path.join(tempfile.mkdtemp(), "h.db")
db = DatabaseManager(path, pool_size=2)

h = db.get_database_health()
check(h['status'] == 'healthy' and h['can_write'], f"normal database: healthy, can_write -> {h['status']}")
check(db.get_bot_state('health_check') is not None, "the check left its timestamp row")

# Write lock held elsewhere past busy_timeout -> the write test fails
blocker = sqlite3.connect(path, isolation_level=None)
blocker.execute("BEGIN IMMEDIATE")
h = db.get_database_health()
blocker.execute("ROLLBACK"); blocker.close()
check(h['status'] == 'unhealthy' and h['can_read'] and not h['can_write'],
      f"database locked for writing: unhealthy, can_read but not can_write -> {h['status']}, {h.get('error')}")

# Read-only database file -> the write test fails
db.close_pool()
os.chmod(path, stat.S_IREAD)
ro = DatabaseManager.__new__(DatabaseManager)           # skip __init__ (it would write the schema)
ro.db_file, ro.pool_size = path, 1
from queue import Queue
ro._pool = Queue(maxsize=1)
h = ro.get_database_health()
os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
check(h['status'] == 'unhealthy' and h['can_read'] and not h['can_write'],
      f"read-only file: unhealthy, can_read but not can_write -> {h['status']}, {h.get('error')}")

db2 = DatabaseManager(path, pool_size=1)
check(db2.get_database_health()['status'] == 'healthy', "healthy again once writable")
db2.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

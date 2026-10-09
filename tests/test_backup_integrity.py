"""Backups are integrity-checked: a corrupt copy is discarded and old backups survive (2026-10 bit flip)."""
import os, sys, tempfile, sqlite3, logging, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

G, U = 753251036982149231, 901351179223711775
tmp = tempfile.mkdtemp()
path, folder = os.path.join(tmp, "b.db"), os.path.join(tmp, "backups")

db = DatabaseManager(path, pool_size=2)
db.add_guild(G, "Hub")
with db.get_connection() as c:
    c.execute("INSERT INTO members (guild_id,user_id,username,roles) VALUES (?,?,'irun','[]')", (G, U))
good = db.create_backup(folder)
check(good is not None and os.path.exists(good), "healthy database: backup kept")
db.close_pool()

# Reproduce the prod corruption: flip one bit of guild_id in the members table
# page only, so the indexes still hold the original value.
with sqlite3.connect(path) as c:
    root = c.execute("SELECT rootpage FROM sqlite_master WHERE name='members'").fetchone()[0]
    page_size = c.execute("PRAGMA page_size").fetchone()[0]
with open(path, "r+b") as f:
    f.seek((root - 1) * page_size); page = bytearray(f.read(page_size))
    at = page.index(G.to_bytes(8, "big")) + 2
    page[at] ^= 0x08
    f.seek((root - 1) * page_size); f.write(page)
with sqlite3.connect(path) as c:
    check(c.execute("PRAGMA quick_check").fetchone()[0] == "ok", "quick_check misses this corruption (why integrity_check is used)")
    check(c.execute("PRAGMA integrity_check").fetchone()[0] != "ok", "integrity_check detects it")

time.sleep(1.1)  # backup filenames have one-second resolution
db = DatabaseManager(path, pool_size=2)
check(db.create_backup(folder) is None, "corrupt database: create_backup returns None")
check(os.listdir(folder) == [os.path.basename(good)], f"corrupt copy deleted, old backup kept {os.listdir(folder)}")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

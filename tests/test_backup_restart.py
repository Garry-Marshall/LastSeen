"""Restarts don't take backups before the interval is up (issue 23)."""
import asyncio, os, sys, time, tempfile, logging
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from database import DatabaseManager
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

tmp = Path(tempfile.mkdtemp())
folder = tmp / "backups"; folder.mkdir()
db = DatabaseManager(str(tmp / "live.db"), pool_size=2)
config = SimpleNamespace(backup_interval_hours=24, backup_retention_count=3, backup_folder=folder)

# Three good backups: 2h, 26h and 50h old
now = time.time()
good = []
for i, age_h in enumerate((2, 26, 50)):
    p = folder / f"lastseen_backup_2026010{i}_000000.db"
    p.write_bytes(b"good")
    os.utime(p, (now - age_h * 3600, now - age_h * 3600))
    good.append(p.name)

async def restart():
    cog = TrackingCog.__new__(TrackingCog)   # fresh process: no in-memory state
    cog.db, cog.config = db, config
    await TrackingCog.backup_database.coro(cog)
    await asyncio.sleep(1.05)               # backup filenames have 1-second resolution

async def main():
    for _ in range(10):                      # crash loop: 10 quick restarts
        await restart()
    files = sorted(p.name for p in folder.glob("lastseen_backup_*.db"))
    check(files == sorted(good), f"10 restarts within the interval: no new backups, good ones kept ({len(files)} files)")

    newest = folder / good[0]
    os.utime(newest, (now - 25 * 3600, now - 25 * 3600))   # interval now passed
    await restart()
    files = sorted(p.name for p in folder.glob("lastseen_backup_*.db"))
    new = [f for f in files if f not in good]
    check(len(new) == 1 and len(files) == 3 and good[2] not in files,
          f"after the interval: 1 new backup, oldest rotated out ({len(files)} files)")
    await restart()
    check(len(list(folder.glob("lastseen_backup_*.db"))) == 3 and
          len([p for p in folder.glob("lastseen_backup_*.db") if p.name not in good]) == 1,
          "restart right after that backup: nothing new")

    for p in folder.glob("*"): p.unlink()
    await restart()
    check(len(list(folder.glob("lastseen_backup_*.db"))) == 1, "empty backup folder: a backup is taken right away")

asyncio.run(main())
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

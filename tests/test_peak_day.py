"""Peak day must be the guild-local weekday, including west of UTC (issue 5)."""
import os, sys, tempfile, logging
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "p.db"), pool_size=2)
G = 1
db.add_guild(G, "G")
for uid in (1, 2):
    db.add_member(G, uid, f"u{uid}", None, 0, [])

# Most recent Tuesday 01:00 UTC at least a day ago (= Monday 21:00 in New York)
now = datetime.now(timezone.utc)
tue = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
while tue.weekday() != 1:
    tue -= timedelta(days=1)
late = int((tue + timedelta(hours=1)).timestamp())    # Tue 01:00 UTC -> Mon 21:00 NY, Tue 10:00 Tokyo
day = int((tue + timedelta(hours=14)).timestamp())    # Tue 14:00 UTC -> Tue 10:00 NY, Tue 23:00 Tokyo
db.increment_message_activity_hourly(G, 1, late, 1, 10)
db.increment_message_activity_hourly(G, 2, day, 14, 3)
db.increment_message_activity(G, 1, int(tue.timestamp()), 10)   # daily rows (no longer used here)
db.increment_message_activity(G, 2, int(tue.timestamp()), 3)

ny = db.get_activity_by_day(G, 30, tz_str='America/New_York')
check(ny['Monday'] == 10 and ny['Tuesday'] == 3, f"New York: Mon 10, Tue 3 -> {ny['Monday']}, {ny['Tuesday']}")
utc = db.get_activity_by_day(G, 30, tz_str='UTC')
check(utc['Tuesday'] == 13 and sum(utc.values()) == 13, f"UTC: all 13 on Tuesday -> {utc}")
tok = db.get_activity_by_day(G, 30, tz_str='Asia/Tokyo')
check(tok['Tuesday'] == 13, f"Tokyo: all 13 on Tuesday -> {tok['Tuesday']}")
bad = db.get_activity_by_day(G, 30, tz_str='Not/AZone')
check(bad['Tuesday'] == 13, "invalid timezone falls back to UTC")
flt = db.get_activity_by_day(G, 30, user_ids=[2], tz_str='America/New_York')
check(sum(flt.values()) == 3 and flt['Tuesday'] == 3, f"track_only_roles filter applied -> {flt}")
old = int((now - timedelta(days=40)).timestamp()) // 3600 * 3600
db.increment_message_activity_hourly(G, 1, old, 0, 99)
check(sum(db.get_activity_by_day(G, 30).values()) == 13, "rows older than the period excluded")
peak = max(ny.items(), key=lambda x: x[1])
check(peak == ('Monday', 10), f"report peak day for New York guild -> {peak}")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

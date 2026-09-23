"""Never-seen members: 'Active (30d)' and retention 'active recently' agree with /inactive."""
import os, sys, random, tempfile, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

D = 86400; NOW = int(datetime.now(timezone.utc).timestamp())
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "n.db"), pool_size=2)

def guild(gid, added_days_ago, members):
    db.add_guild(gid, f"G{gid}")
    with db.get_connection() as c:
        c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - added_days_ago * D, gid))
        c.executemany("INSERT INTO members (guild_id,user_id,username,join_date,last_seen,is_active,roles) VALUES (?,?,?,?,?,1,'[]')",
                      [(gid, uid, f"u{uid}", NOW - j * D, ls) for uid, j, ls in members])

# Guild with the bot for 400 days
guild(1, 400, [
    (1, 300, 0),               # online now                       -> active
    (2, 300, NOW - 5 * D),     # seen 5 days ago                  -> active
    (3, 300, NOW - 40 * D),    # seen 40 days ago                 -> inactive
    (4, 300, None),            # never seen, joined 300 days ago  -> inactive (was counted active)
    (5, 5, None),              # never seen, joined 5 days ago    -> active (grace, like /inactive)
])
s = db.get_server_snapshot_stats(1)
check((s['active_30d'], s['inactive_30d']) == (3, 2), f"overview: 3 active, 2 inactive -> {s['active_30d']}, {s['inactive_30d']}")

# Guild added 10 days ago: a never-seen long-time member is still in the grace period
guild(2, 10, [(10, 700, None), (11, 700, NOW - 3 * D)])
s = db.get_server_snapshot_stats(2)
check(s['active_30d'] == 2, f"young guild: never-seen member within grace counts active -> {s['active_30d']}")

# Property: for random members, active_30d == members not listed by /inactive 30
random.seed(7)
rows = []
for uid in range(100, 400):
    joined = random.randint(0, 900)
    ls = random.choice([0, None, NOW - random.randint(0, 200) * D, NOW - 30 * D])
    rows.append((uid, joined, ls))
for added in (5, 29, 30, 31, 400):
    gid = 1000 + added
    guild(gid, added, rows)
    active = db.get_server_snapshot_stats(gid)['active_30d']
    inactive = len(db.get_inactive_members(gid, 30))
    check(active + inactive == len(rows), f"bot added {added:>3}d ago: {active} active + {inactive} /inactive = {len(rows)} members")

# Retention cohorts use the same rule
guild(3, 200, [(20 + i, 45, None) for i in range(3)] + [(30 + i, 45, NOW - D) for i in range(3)])
c = db.get_retention_cohorts(3)['60d']
check(c['total_joined'] == 6 and c['active_recently'] == 3, f"cohort 'active recently' excludes never-seen 45-day members -> {c['active_recently']}")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

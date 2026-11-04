"""A report in a guild with track_only_roles never uses excluded members.

Builds the same tracked members twice: once alongside busy excluded members
with the role filter set, once alone with no filter. If every section honours
the filter, the two reports are identical.
"""
import os, sys, tempfile, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales
from database import DatabaseManager
from bot.reports import generate_activity_report

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

D = 86400
NOW = int(datetime.now(timezone.utc).timestamp())
TODAY = NOW - NOW % D
G = 1
TYPES = ['members', 'departures', 'retention']


def add(db, uid, role, joined_days_ago, posts, hour, left_days_ago=None, returned=False):
    """One member: posts messages/day at the given UTC hour on each of the last 14 days."""
    db.add_member(G, uid, f"u{uid}", None, NOW - joined_days_ago * D, [role])
    db.update_last_seen(G, uid, NOW - 2 * D)
    for day in range(1, 15):
        if posts:
            db.increment_message_activity(G, uid, TODAY - day * D, posts)
            db.increment_message_activity_hourly(G, uid, TODAY - day * D + hour * 3600, hour, posts)
    if left_days_ago is not None:
        with db.get_connection() as c:
            c.execute("UPDATE members SET is_active = 0, left_date = ? WHERE guild_id = ? AND user_id = ?",
                      (NOW - left_days_ago * D, G, uid))
    if returned:
        db.add_member_return(G, uid, 40 * D, NOW - 3 * D)


def build(with_excluded):
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "f.db"), pool_size=2)
    db.add_guild(G, "Filtered")
    with db.get_connection() as c:  # old enough for monthly trends and every cohort
        c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - 100 * D, G))
    # Tracked: a modest, evening-posting group with joins, departures and a return.
    for i in range(8):
        add(db, 100 + i, 'Staff', 40 + i, posts=2 if i % 2 == 0 else 0, hour=20,
            left_days_ago=5 if i < 3 else None, returned=(i == 4))
    if with_excluded:
        # Excluded: a much busier morning crowd with its own joins/leaves/returns,
        # so any section that ignores the filter changes the report.
        for i in range(25):
            add(db, 500 + i, 'Guest', 3 + i * 3, posts=9, hour=8,
                left_days_ago=2 if i % 4 == 0 else None, returned=(i % 5 == 0))
        db.set_track_only_roles(G, ['Staff'], "Filtered")
    return db


def report(db, days):
    guild = type("G", (), {"id": G, "name": "Filtered"})()
    return generate_activity_report(guild, db, days, TYPES).description


load_locales()
mixed, alone = build(True), build(False)
for days in (7, 30):
    filtered = report(mixed, days)
    expected = report(alone, days)
    check(filtered == expected, f"{days}d report with role filter matches the tracked members alone")
    if filtered != expected:
        print("--- filtered ---\n" + filtered + "\n--- expected ---\n" + expected)

# Sanity: without the filter the excluded members do change the report, so the
# comparison above is meaningful.
mixed.set_track_only_roles(G, [], "Filtered")
check(report(mixed, 30) != report(alone, 30), "excluded members change an unfiltered report")

for db in (mixed, alone):
    db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

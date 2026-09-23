"""Retention cohorts + activation funnel only count joiners after the bot arrived (issue 8)."""
import os, sys, tempfile, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales, t
from database import DatabaseManager
from bot.reports import generate_activity_report

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

D = 86400
NOW = int(datetime.now(timezone.utc).timestamp())
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "c.db"), pool_size=2)
G = 1
db.add_guild(G, "Young")
with db.get_connection() as c:
    c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - 45 * D, G))

uid = 0
def member(joined_days_ago, active=True, posts_on_join=False):
    global uid; uid += 1
    jd = NOW - joined_days_ago * D
    db.add_member(G, uid, f"u{uid}", None, jd, [])
    if not active:
        with db.get_connection() as c:
            c.execute("UPDATE members SET is_active = 0, left_date = ? WHERE guild_id = ? AND user_id = ?", (NOW - D, G, uid))
    if posts_on_join:
        db.increment_message_activity(G, uid, jd - jd % D, 3)

for _ in range(6): member(70)                 # pre-arrival survivors (90d cohort), never posted since
for _ in range(4): member(50)                 # pre-arrival survivors (60d cohort)
for i in range(6): member(35 + i, active=(i % 2 == 0), posts_on_join=True)   # post-arrival: 3 stay, 3 left
for _ in range(3): member(10)                 # post-arrival, but only 3 -> below minimum

cohorts = db.get_retention_cohorts(G)
check(set(cohorts) == {'60d'}, f"only the 60d cohort is shown -> {sorted(cohorts)}")
c60 = cohorts.get('60d', {})
check(c60.get('total_joined') == 6 and c60.get('still_active') == 3 and round(c60.get('retention_rate', 0)) == 50,
      f"60d cohort excludes pre-arrival survivors: 3/6 = 50% (was 7/10 = 70%) -> {c60}")
check('90d' not in cohorts, "90d cohort (entirely before the bot) hidden (was 6/6 = 100%)")
check('30d' not in cohorts, "30d cohort with 3 joiners hidden by the minimum of 5")

funnel = db.get_activation_funnel(G)
check(funnel['cohort_size'] == 9, f"funnel cohort = 9 post-arrival joiners, not 19 -> {funnel['cohort_size']}")
d1 = next((cp for cp in funnel['checkpoints'] if cp['label'] == 'D1'), None)
check(d1 and d1['matured'] == 9 and d1['active'] == 6 and round(d1['rate']) == 67,
      f"D1 activation 6/9 = 67% (was 6/19 = 32%) -> {d1}")

# Guild added long ago: nothing filtered, all three cohorts shown when big enough
G2 = 2
db.add_guild(G2, "Old")
with db.get_connection() as c:
    c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - 400 * D, G2))
    for i, days in enumerate([10] * 5 + [40] * 5 + [70] * 5):
        c.execute("INSERT INTO members (guild_id,user_id,username,join_date,is_active,roles) VALUES (?,?,?,?,1,'[]')",
                  (G2, 1000 + i, "x", NOW - days * D))
check(set(db.get_retention_cohorts(G2)) == {'30d', '60d', '90d'}, "established guild: all three cohorts, unchanged")

# Monthly report retention section follows the same rules
load_locales()
guild = type("G", (), {"id": G, "name": "Young"})()
desc = generate_activity_report(guild, db, 30, ['activity', 'retention']).description
check(t('report.retention_period_60d', 'en') in desc and t('report.retention_period_90d', 'en') not in desc,
      "report shows the 60d line only")
empty_guild = 3; db.add_guild(empty_guild, "Empty")
desc = generate_activity_report(type("G", (), {"id": 3, "name": "Empty"})(), db, 30, ['activity', 'retention']).description
check(t('report.retention_header', 'en') not in desc, "report omits the retention section when no cohort qualifies")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

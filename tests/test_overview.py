"""/user-stats overview: total = current members, growth = last 30 days (issue 9)."""
import os, sys, re, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales
from database import DatabaseManager
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

load_locales()
D = 86400; NOW = int(datetime.now(timezone.utc).timestamp())
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "o.db"), pool_size=2)
G = 1; db.add_guild(G, "G")
with db.get_connection() as c:
    c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - 400 * D, G))
    rows = []
    for u in range(1, 9):   # 8 current members: 6 seen recently, 2 seen 60 days ago
        rows.append((G, u, f"u{u}", NOW - 300 * D, NOW - (2 if u <= 6 else 60) * D, 1, None))
    rows[0] = (G, 1, "u1", NOW - 10 * D, NOW - D, 1, None)      # joined 10 days ago
    rows[1] = (G, 2, "u2", NOW - 45 * D, NOW - D, 1, None)      # joined 45 days ago (in 60d, not 30d)
    for u in range(9, 13):  # 4 departed long ago
        rows.append((G, u, f"u{u}", NOW - 300 * D, NOW - 200 * D, 0, NOW - 200 * D))
    c.executemany("INSERT INTO members (guild_id,user_id,username,join_date,last_seen,is_active,left_date,roles) VALUES (?,?,?,?,?,?,?,'[]')", rows)

stats = db.get_server_snapshot_stats(G)
check('total_members' not in stats and stats['active_members'] == 8, f"snapshot: 8 current members {stats['active_members']}")
g30 = db.get_member_growth_stats(G, 30)['growth_rate']
g60 = db.get_member_growth_stats(G, 60)['growth_rate']
check(round(g30, 1) == 14.3 and round(g60, 1) == 33.3, f"growth 30d {g30:.1f}% (1 join on 7) vs 60d {g60:.1f}% (2 joins on 6)")

stats['guild_id'] = G
desc = CommandsCog._create_stats_overview_embed(SimpleNamespace(db=db), stats, g30, 'en').description
print("   " + desc.replace("\n", "\n   "))
check("Total Members:** 8 " in desc, "overview total = 8 current members (was 12 incl. departed)")
pcts = [float(p) for p in re.findall(r"\((\d+\.\d)%\)", desc)]
check(pcts == [75.0, 25.0], f"active/inactive split sums to 100% -> {pcts}")
check("14.3% vs last month" in desc, "'vs last month' shows the 30-day rate")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

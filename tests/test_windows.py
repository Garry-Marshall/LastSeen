"""Every "last N days" message count covers the same N calendar days (issue 25)."""
import os, sys, tempfile, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from database import DatabaseManager
from database.db_manager import _day_window_start

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

D = 86400
now = datetime.now(timezone.utc)
TODAY = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
G, U = 1, 42

db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "w.db"), pool_size=2)
db.add_guild(G, "G")
with db.get_connection() as c:   # the bot has been in this server long before the 40 days of data
    c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (TODAY - 400 * D, G))
db.add_member(G, U, "poster", None, TODAY - 100 * D, [])
for i in range(5):   # a few idle members so the percentile has a population
    db.add_member(G, 100 + i, f"idle{i}", None, TODAY - 100 * D, [])
for d in range(40):  # 10 messages on each of the last 40 days, today included
    db.increment_message_activity(G, U, TODAY - d * D, 10)

check(_day_window_start(30) == TODAY - 29 * D and _day_window_start(1) == TODAY, "window = today + N-1 previous days")

counts_30 = {
    "/whois & /mystats (get_message_activity_period)": db.get_message_activity_period(G, U, 30)['total'],
    "/mystats percentile (caller total)": db.get_activity_percentile(G, U, 30)['caller_total'],
    "/search (get_guild_activity_totals)": db.get_guild_activity_totals(G, 30)[U]['total'],
    "/whois sparkline & trend (sum)": sum(r['message_count'] for r in db.get_message_activity_trend(G, U, 30)),
    "/user-stats leaderboard": db.get_activity_leaderboard(G, 30)[0]['total_messages'],
    "report top contributors": db.get_top_active_users_period(G, 30)[0]['total_messages'],
    "/chat-history server 30d": db.get_guild_message_activity_stats(G, 30)['total_30d'],
    "/user-stats overview 30d": db.get_server_snapshot_stats(G)['total_messages_30d'],
}
for name, v in counts_30.items():
    print(f"     {name:48s} {v}")
check(set(counts_30.values()) == {300}, "every '30 days' count = 30 days x 10 = 300")

counts_7 = {
    "/whois 'this week'": db.get_message_activity_period(G, U, 30)['this_week'],
    "/search 7d": db.get_guild_activity_totals(G, 30)[U]['this_week'],
    "/chat-history server 7d": db.get_guild_message_activity_stats(G, 30)['total_7d'],
    "/about last 7 days": db.get_bot_statistics()['last_7d'],
}
check(set(counts_7.values()) == {70}, f"every '7 days' count = 70 -> {counts_7}")
check(db.get_message_activity_period(G, U, 30)['avg_per_day'] == 10.0, "7-day daily average = exactly 10/day (was 80/7 = 11.4)")
check(db.get_guild_message_activity_stats(G, 30)['avg_per_day'] == 10.0, "30-day daily average = exactly 10/day")
check(sum(r['message_count'] for r in db.get_message_activity_trend(G, U, 365)) == 400, "365-day history counts the 40 real days")

# Participation windows use the same days: a member whose only message is
# exactly 30 days ago (outside today + 29 days) now counts as silent.
db.add_member(G, 7, "boundary", None, TODAY - 100 * D, [])
db.update_last_seen(G, 7, 0)
db.increment_message_activity(G, 7, TODAY - 30 * D, 5)
lurkers = {m['user_id'] for m in db.get_lurkers(G, 30)}
check(7 in lurkers, "message 30 days ago is outside the 30-day window -> lurker (was counted as active)")
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

"""Per-day averages divide by the days that could hold data; 'this month' is UTC (issue 26)."""
import asyncio, os, re, sys, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from bot.locale import load_locales
from database import DatabaseManager
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

D = 86400
now = datetime.now(timezone.utc)
TODAY = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
MONTH_START = int(datetime(now.year, now.month, 1, tzinfo=timezone.utc).timestamp())

db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "a.db"), pool_size=2)

def guild(gid, added_days_ago, retention=365):
    db.add_guild(gid, f"G{gid}")
    with db.get_connection() as c:
        c.execute("UPDATE guilds SET added_at = ?, message_retention_days = ? WHERE guild_id = ?",
                  (TODAY - added_days_ago * D, retention, gid))

def member(gid, uid, joined_days_ago, posts_days):
    db.add_member(gid, uid, f"u{uid}", None, TODAY - joined_days_ago * D + 3600, [])
    for d in posts_days:
        db.increment_message_activity(gid, uid, TODAY - d * D, 10)

guild(1, 1000)
member(1, 1, 19, range(20))            # joined 19 days ago (20 calendar days incl. today), 10/day
member(1, 2, 2000, range(365))         # long-time member, 10/day all year
member(1, 3, 4, range(30))             # rejoined 4 days ago, but has messages for 30 days
member(1, 4, 1, range(2))              # joined yesterday, posted both days
days = lambda gid, uid, n: db.get_member_days_with_data(gid, uid, n)
check(days(1, 1, 365) == 20, f"recent joiner: 20 days, not 365 -> {days(1, 1, 365)}")
check(days(1, 2, 365) == 365, f"long-time member: full 365 days -> {days(1, 2, 365)}")
check(days(1, 3, 365) == 30, f"rejoiner: counted from first message (30), not latest join (5) -> {days(1, 3, 365)}")
check(db.get_message_activity_period(1, 4, 30)['avg_per_day'] == 10.0,
      f"/whois 7-day average for a 2-day member: 10.0 (was 20/7 = 2.9) -> {db.get_message_activity_period(1, 4, 30)['avg_per_day']}")

guild(2, 9)                            # bot added 9 days ago (10 calendar days)
member(2, 20, 500, range(10))
check(db.get_guild_message_activity_stats(2, 30)['avg_per_day'] == 10.0,
      f"server added 10 days ago: 30-day average 10.0 (was 100/30 = 3.3) -> {db.get_guild_message_activity_stats(2, 30)['avg_per_day']}")

guild(3, 1000, retention=90)
member(3, 30, 2000, range(90))
check(days(3, 30, 365) == 90, f"90-day retention: at most 90 days -> {days(3, 30, 365)}")


async def chat_history_text(gid, uid):
    sent = []
    async def reply(*a, **kw): sent.append(kw)
    async def defer(**kw): pass
    user = SimpleNamespace(id=99, guild_permissions=SimpleNamespace(administrator=True), roles=[])
    interaction = SimpleNamespace(guild_id=gid, channel_id=5, channel=None, user=user,
                                  guild=SimpleNamespace(name="G", members=[]),
                                  response=SimpleNamespace(defer=defer, send_message=reply),
                                  followup=SimpleNamespace(send=reply))
    await CommandsCog.chat_history.callback(CommandsCog(None, db, None), interaction, user=f"u{uid}")
    return sent[-1]['embed'].description

async def main():
    load_locales()
    text = await chat_history_text(1, 1)
    avg = re.search(r"Average/Day: \*\*([\d.]+)", text).group(1)
    check(avg == "10.0", f"/chat-history recent joiner shows 10.0/day (was 200/365 = 0.5) -> {avg}")

    # 'This month' boundary: 7 messages on the 1st of this UTC month, 3 on the last day of the previous one
    guild(4, 1000)
    db.add_member(4, 40, "u40", None, TODAY - 400 * D, [])
    db.increment_message_activity(4, 40, MONTH_START, 7)
    db.increment_message_activity(4, 40, MONTH_START - D, 3)
    text = await chat_history_text(4, 40)
    month = re.search(r"This month: \*\*(\d+)", text).group(1)
    check(month == "7", f"'This month' counts only this UTC month's days -> {month}")

asyncio.run(main())
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

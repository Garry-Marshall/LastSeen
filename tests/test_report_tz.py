"""Scheduled reports: an invalid guild timezone falls back to UTC cleanly (issue 15)."""
import asyncio, os, sys, tempfile, logging
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import bot.reports
from database import DatabaseManager
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)


async def main():
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "t.db"), pool_size=2)
    now = datetime.now(timezone.utc)
    yesterday = int((now - timedelta(days=1)).timestamp())
    # Guild ids are multiples of 10, so the per-guild jitter minute is 0.
    guilds = {
        10: ('Mars/Olympus_Mons', yesterday),   # invalid tz, processed first
        20: ('Europe/Amsterdam', yesterday),    # valid tz
        30: ('Not/AZone', yesterday),           # invalid tz after a valid one
        40: ('Mars/Olympus_Mons', int(now.timestamp()) - 60),  # invalid tz, already sent today
    }
    for gid, (tz, last) in guilds.items():
        db.add_guild(gid, f"G{gid}")
        local_weekday = now.astimezone(__import__('pytz').timezone(tz)).weekday() if '/' in tz and tz.startswith('Europe') else now.weekday()
        db.set_report_config(gid, 555, 'weekly', ['activity'], day_weekly=local_weekday, time_hour=0)
        with db.get_connection() as c:
            c.execute("UPDATE guilds SET timezone = ?, last_weekly_report = ? WHERE guild_id = ?", (tz, last, gid))

    sent = []
    async def fake_send(guild, channel_id, db_, report_types, days):
        sent.append(guild.id); return True
    bot.reports.send_scheduled_report = fake_send

    cog = TrackingCog.__new__(TrackingCog)
    cog.db = db
    cog.bot = SimpleNamespace(get_guild=lambda gid: SimpleNamespace(id=gid, name=f"G{gid}") if gid in guilds else None)
    await TrackingCog.check_scheduled_reports.coro(cog)

    check(10 in sent, "invalid timezone as the first guild: report sent in UTC (NameError before)")
    check(20 in sent, "valid timezone: report sent")
    check(30 in sent, "invalid timezone after a valid one: report sent in UTC")
    check(40 not in sent, "invalid timezone, already sent today: dedup still works in UTC")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

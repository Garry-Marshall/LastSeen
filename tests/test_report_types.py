"""A report configured without 'activity' must still be sent (issue 4)."""
import asyncio, os, sys, tempfile, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
import discord
from bot.locale import load_locales
from database import DatabaseManager
import bot.reports as reports

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

class FakeChannel(discord.TextChannel):
    def __init__(self): self.sent = []; self.name = "reports"
    async def send(self, embed=None): self.sent.append(embed)

async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "r.db"), pool_size=2)
    now = int(datetime.now(timezone.utc).timestamp())
    for gid in (1, 2, 3):
        db.add_guild(gid, f"G{gid}")
        db.add_member(gid, 10, "joiner", None, now - 3600, [])
    for gid, types, label in [(1, ['members', 'departures'], "without activity"),
                              (2, ['activity', 'members'], "with activity"),
                              (3, ['retention'], "retention only")]:
        ch = FakeChannel()
        guild = type("G", (), {"id": gid, "name": f"G{gid}", "get_channel": lambda self, cid, ch=ch: ch})()
        reports._last_report_send.clear()
        ok = await reports.send_scheduled_report(guild, 99, db, types, 7)
        desc = ch.sent[0].description if ch.sent else ""
        check(ok and len(ch.sent) == 1, f"{label}: report sent")
        check("Total messages" in desc or "messages" in desc.lower(), f"{label}: activity overview present")
        has_members = "Joined" in desc
        check(has_members == ('members' in types), f"{label}: members section {'shown' if has_members else 'hidden'}")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

"""/chat-history: server-wide open; a member's history for admins or themselves (issue 27)."""
import asyncio, os, sys, tempfile, logging
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
db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "c.db"), pool_size=2)
for gid in (1, 2):
    db.add_guild(gid, f"G{gid}")
    for uid, name in ((10, "alice"), (20, "bob")):
        db.add_member(gid, uid, name, None, TODAY - 50 * D, [])
        db.increment_message_activity(gid, uid, TODAY, 5)
db.set_allowed_channels(2, [5])      # guild 2: results are posted publicly


async def run(gid, caller_id, admin, user):
    calls = []
    async def send_message(*a, **kw): calls.append(('response', kw))
    async def defer(**kw): calls.append(('defer', kw))
    async def followup(*a, **kw): calls.append(('followup', kw))
    caller = SimpleNamespace(id=caller_id, guild_permissions=SimpleNamespace(administrator=admin), roles=[])
    interaction = SimpleNamespace(guild_id=gid, channel_id=5, channel=None, user=caller,
                                  guild=SimpleNamespace(name=f"G{gid}", members=[]),
                                  response=SimpleNamespace(defer=defer, send_message=send_message),
                                  followup=SimpleNamespace(send=followup))
    await CommandsCog.chat_history.callback(CommandsCog(None, db, None), interaction, user=user)
    return calls

def title(calls):
    last = calls[-1][1].get('embed')
    return last.title if last else ""

async def main():
    load_locales()
    c = await run(1, 10, False, None)
    check(any(k == 'defer' for k, _ in c) and "Server" in title(c), "member: server-wide stats allowed")
    c = await run(1, 10, False, "alice")
    check(c[-1][0] == 'followup' and "alice" in title(c), "member: own history allowed")
    c = await run(1, 10, False, "bob")
    check(len(c) == 1 and c[0][0] == 'response' and c[0][1].get('ephemeral') and "Error" in title(c),
          "member: another member's history refused privately, nothing else sent")
    check("You can always view your own" in c[0][1]['embed'].description, "refusal explains the rule")
    c = await run(1, 99, True, "bob")
    check(c[-1][0] == 'followup' and "bob" in title(c), "admin: anyone's history allowed")
    c = await run(2, 10, False, "bob")
    check(len(c) == 1 and c[0][1].get('ephemeral'), "public-results server: refusal still private")
    c = await run(2, 10, False, "alice")
    defer_kw = next(kw for k, kw in c if k == 'defer')
    check(defer_kw.get('ephemeral') is False and "alice" in title(c), "public-results server: own history posted as before")
    c = await run(1, 10, False, "nobody")
    check(c[-1][0] == 'followup' and "Error" in title(c), "unknown member: normal not-found message")

asyncio.run(main())
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

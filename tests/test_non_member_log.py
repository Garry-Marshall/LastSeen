"""Message authors without member data are logged, with metadata only."""
import asyncio, os, sys, logging
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import MagicMock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

G, UID = 1, 42
lines = []
class Capture(logging.Handler):
    def emit(self, record):
        lines.append(record.getMessage())
log = logging.getLogger('cogs.tracking')
log.addHandler(Capture()); log.setLevel(logging.INFO); log.propagate = False

cog = TrackingCog.__new__(TrackingCog)
cog.bot = SimpleNamespace(opted_out_users=set())
cog.daily_activity_buffer = defaultdict(int)
cog.hourly_activity_buffer = defaultdict(int)
cog.MAX_BUFFER_SIZE = 1000
cog._flush_lock = asyncio.Lock()

non_member = discord.User(state=None, data={'id': str(UID), 'username': 'ghost', 'discriminator': '0',
                                             'avatar': None, 'public_flags': 64})
provisional = discord.User(state=None, data={'id': str(UID), 'username': 'whimsical_eagle', 'discriminator': '0',
                                              'avatar': None, 'public_flags': 1 << 23})
member = MagicMock(spec=discord.Member, id=UID, bot=False)


def message(author, **overrides):
    fields = dict(author=author, guild=SimpleNamespace(id=G, name="Guild"), webhook_id=None,
                  type=discord.MessageType.default, application_id=777, content="SECRET",
                  channel=SimpleNamespace(type=discord.ChannelType.text))
    return SimpleNamespace(**{**fields, **overrides})


async def send(msg):
    lines.clear()
    cog.daily_activity_buffer.clear()
    await TrackingCog.on_message(cog, msg)
    return [l for l in lines if 'non-member' in l]


async def main():
    logged = await send(message(non_member))
    check(len(logged) == 1, f"non-member author logged once ({len(logged)})")
    line = logged[0] if logged else ""
    for part in (str(UID), "Guild", "application_id=777", "public_flags=64", "MessageType.default", "text"):
        check(part in line, f"line contains {part}")
    check("SECRET" not in line, "message content is never logged")
    check(sum(cog.daily_activity_buffer.values()) == 1, "message is still buffered as before")

    check(not await send(message(provisional)), "provisional account: not logged")
    check(not cog.daily_activity_buffer and not lines, "provisional account: skipped, nothing buffered")

    check(not await send(message(member)), "guild member: not logged")
    check(sum(cog.daily_activity_buffer.values()) == 1, "guild member: still buffered")
    check(not await send(message(non_member, webhook_id=5)), "webhook: not logged")
    check(not await send(message(non_member, guild=None)), "DM: not logged")

    cog.bot.opted_out_users.add(UID)
    check(not await send(message(non_member)), "opted-out author: not logged")


asyncio.run(main())
sys.exit(1 if failures else 0)

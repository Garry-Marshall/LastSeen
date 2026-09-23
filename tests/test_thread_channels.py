"""Threads inside an allowed channel are allowed (issue 29)."""
import asyncio, os, sys, json, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.locale import load_locales
from bot.utils import is_channel_allowed
from database import DatabaseManager
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

ALLOWED, OTHER = 100, 200
cfg = {'allowed_channels': json.dumps([ALLOWED])}
check(is_channel_allowed(ALLOWED, cfg), "allowed channel itself")
check(not is_channel_allowed(OTHER, cfg), "other channel refused")
check(is_channel_allowed(555, cfg, parent_id=ALLOWED), "thread inside the allowed channel: allowed (was refused)")
check(not is_channel_allowed(556, cfg, parent_id=OTHER), "thread inside another channel: refused")
check(is_channel_allowed(OTHER, {'allowed_channels': None}, parent_id=None), "no restriction: everything allowed")

# Only threads carry parent_id in discord.py; normal channels have category_id instead
slots = lambda cls: {s for k in cls.__mro__ for s in getattr(k, '__slots__', ())}
check('parent_id' in slots(discord.Thread) and 'parent_id' not in slots(discord.TextChannel)
      and 'parent_id' not in slots(discord.VoiceChannel), "only discord.Thread has parent_id (a category never counts)")

async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "t.db"), pool_size=2)
    db.add_guild(1, "G")
    db.set_allowed_channels(1, [ALLOWED])
    cog = CommandsCog(None, db, None)
    user = SimpleNamespace(id=1, guild_permissions=SimpleNamespace(administrator=False), roles=[])
    def interaction(channel_id, channel):
        return SimpleNamespace(guild_id=1, channel_id=channel_id, channel=channel, user=user)
    ok_thread = (await cog._check_permissions(interaction(555, SimpleNamespace(parent_id=ALLOWED))))[0]
    bad_thread = (await cog._check_permissions(interaction(556, SimpleNamespace(parent_id=OTHER))))[0]
    plain = (await cog._check_permissions(interaction(ALLOWED, SimpleNamespace())))[0]
    check(ok_thread and not bad_thread and plain, "command permission check: thread in allowed channel passes, others as before")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

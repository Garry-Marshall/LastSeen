"""A new guild's member enumeration survives a hung or failing chunk request:
it retries, never enumerates a partial cache, and on giving up becomes
retryable by the next on_ready instead of staying stuck in _enumerating."""
import asyncio, os, sys, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from database import DatabaseManager
import cogs.tracking as tracking
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
tracking.CHUNK_TIMEOUT = 0.05
tracking.ENUM_CHUNK_RETRY_DELAY = 0


class FakeRole:
    def __init__(self, name): self.name = name


class FakeMember:
    def __init__(self, uid):
        self.id, self.name, self.display_name, self.bot = uid, f"user{uid}", f"user{uid}", False
        self.joined_at = datetime.fromtimestamp(NOW - 86400 * uid, tz=timezone.utc)
        self.roles = [FakeRole('@everyone')]
        self.status = discord.Status.offline

    def __str__(self): return self.name


class FakeGuild:
    """chunk() follows a script: 'hang', 'fail' or 'ok' per call."""
    def __init__(self, gid, script):
        self.id, self.name, self.script = gid, f"G{gid}", list(script)
        self.chunked, self.members, self.calls = False, [], 0

    async def chunk(self):
        self.calls += 1
        step = self.script.pop(0)
        if step == 'hang':
            await asyncio.Event().wait()
        if step == 'fail':
            raise discord.ClientException("chunk failed")
        self.members = [FakeMember(uid) for uid in (1, 2, 3)]
        self.chunked = True


def make_cog(db):
    cog = TrackingCog.__new__(TrackingCog)
    cog.db = db
    cog.bot = SimpleNamespace(opted_out_users=set())
    cog._enumerating = set()
    return cog


async def run(cog, guild):
    cog._start_member_enumeration(guild)
    for _ in range(200):  # let the background task finish
        await asyncio.sleep(0.01)
        if guild.id not in cog._enumerating or cog.db.guild_positions_initialized(guild.id):
            break


async def main():
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "e.db"), pool_size=2)
    cog = make_cog(db)

    # ---------- 1. chunk hangs every time ----------
    g = FakeGuild(1, ['hang'] * 3)
    db.add_guild(g.id, g.name)
    await run(cog, g)
    check(g.calls == tracking.ENUM_CHUNK_ATTEMPTS, f"hung chunk retried {g.calls} times")
    check(g.id not in cog._enumerating, "gave up -> guild no longer marked as enumerating")
    check(not db.get_all_guild_members(g.id), "nothing enumerated from a partial cache")

    # The next on_ready pass can schedule it again, and this time it works.
    g.script = ['ok']
    await run(cog, g)
    check(len(db.get_all_guild_members(g.id)) == 3, "retry after giving up imports every member")
    check(db.guild_positions_initialized(g.id), "retry initializes join positions")
    check(g.id in cog._enumerating, "successful import stays marked (no re-scan on reconnect)")

    # ---------- 2. chunk errors, then succeeds within the same task ----------
    g = FakeGuild(2, ['fail', 'hang', 'ok'])
    db.add_guild(g.id, g.name)
    await run(cog, g)
    check(g.calls == 3, f"failed + hung chunk retried until success ({g.calls} calls)")
    check(len(db.get_all_guild_members(g.id)) == 3, "members imported after retries")
    check(g.id in cog._enumerating, "guild stays marked after success")

    # ---------- 3. chunk errors every time ----------
    g = FakeGuild(3, ['fail'] * 3)
    db.add_guild(g.id, g.name)
    await run(cog, g)
    check(g.id not in cog._enumerating, "always-failing chunk -> guild retryable")

    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

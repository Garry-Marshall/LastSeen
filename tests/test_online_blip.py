"""online_return ignores brief disconnects (e.g. restarting Discord) via ONLINE_MIN_AWAY_SECONDS."""
import asyncio, os, sys, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales
from database import DatabaseManager
from cogs.watch import WatchCog, ONLINE_MIN_AWAY_SECONDS, ROLE_DIGEST_SECONDS

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
G, ROLE, CHANNEL = 1, 777, 555


class FakeRole:
    def __init__(self, rid): self.id, self.name, self.members = rid, "Staff", []
    def is_default(self): return False


class FakeMember:
    def __init__(self, uid, guild, roles):
        self.id, self.guild, self.roles, self.bot = uid, guild, roles, False
        self.mention = f"<@{uid}>"


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "b.db"), pool_size=2)
    db.add_guild(G, "G")

    sent = []
    channel = SimpleNamespace(id=CHANNEL, name="alerts",
                              permissions_for=lambda me: SimpleNamespace(send_messages=True))
    async def send(embed=None): sent.append(embed)
    channel.send = send
    role = FakeRole(ROLE)
    guild = SimpleNamespace(id=G, name="G", me=None, chunked=True,
                            get_channel_or_thread=lambda cid: channel if cid == CHANNEL else None,
                            get_member=lambda uid: None, get_role=lambda rid: role if rid == ROLE else None)
    bot = SimpleNamespace(opted_out_users=set(), no_watch_users=set(), watch_guild_ids={G},
                          get_guild=lambda gid: guild if gid == G else None)
    cog = WatchCog.__new__(WatchCog)
    cog.db, cog.bot = db, bot
    cog._online_cooldown, cog._role_returns, cog._suppress_online_until = {}, {}, 0

    db.add_watch(G, 'user', 5, 'online_return', None, CHANNEL, 99)
    db.add_watch(G, 'role', ROLE, 'online_return', None, CHANNEL, 99)

    # Discord restart: offline for 20s, then back.
    await cog.on_lastseen_member_online(FakeMember(5, guild, [role]), NOW - 20)
    check(not sent, "user watch: 20s absence does not alert")
    check(not cog._role_returns, "role watch: 20s absence not added to digest")
    check(not cog._online_cooldown, "blip does not consume the cooldown")

    # Just under / at the floor.
    await cog.on_lastseen_member_online(FakeMember(5, guild, [role]), NOW - ONLINE_MIN_AWAY_SECONDS + 5)
    check(not sent, "just under the floor does not alert")
    await cog.on_lastseen_member_online(FakeMember(5, guild, [role]), NOW - ONLINE_MIN_AWAY_SECONDS - 5)
    check(len(sent) == 1 and "<@5>" in sent[0].description, "past the floor: user watch alerts")
    await cog._flush_role_digests(NOW + ROLE_DIGEST_SECONDS + 60)
    check(len(sent) == 2 and "<@5>" in sent[1].description, "past the floor: role digest includes the member")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

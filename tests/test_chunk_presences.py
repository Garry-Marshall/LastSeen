"""Guild chunking requests presences, and the presences in a chunk end up in
the member cache (so reconciliation sees who is really online)."""
import asyncio, os, sys, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.client import LastSeenBot, _chunk_with_presences

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)


def make_bot():
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    bot = LastSeenBot(command_prefix='!', intents=intents, chunk_guilds_at_startup=False)
    _chunk_with_presences(bot._connection)
    return bot


class FakeWS:
    def __init__(self): self.requests = []
    async def request_chunks(self, guild_id, **kwargs): self.requests.append((guild_id, kwargs))


def user(uid): return {'id': str(uid), 'username': f'u{uid}', 'discriminator': '0', 'avatar': None}


async def main():
    bot = make_bot()
    state = bot._connection
    state.loop = asyncio.get_running_loop()  # normally set at login
    ws = FakeWS()
    state._get_websocket = lambda guild_id=None, *, shard_id=None: ws

    guild = discord.Guild(data={'id': '1', 'name': 'G', 'member_count': 3}, state=state)
    state._add_guild(guild)

    # ---------- 1. Guild.chunk() asks for presences ----------
    task = asyncio.ensure_future(guild.chunk())
    await asyncio.sleep(0)
    check(len(ws.requests) == 1, "chunk() sent one request")
    gid, kw = ws.requests[0]
    check(gid == 1 and kw.get('presences') is True, f"request asks for presences ({kw})")
    check(kw.get('query') == '' and kw.get('limit') == 0, "full member list requested")

    # ---------- 2. Presences in the chunk land in the cache ----------
    state.parse_guild_members_chunk({
        'guild_id': '1', 'chunk_index': 0, 'chunk_count': 1, 'nonce': kw['nonce'],
        'members': [{'user': user(u), 'roles': [], 'joined_at': '2024-01-01T00:00:00+00:00',
                     'deaf': False, 'mute': False, 'flags': 0} for u in (10, 11, 12)],
        'presences': [{'user': {'id': '10'}, 'status': 'online', 'activities': [], 'client_status': {'desktop': 'online'}},
                      {'user': {'id': '11'}, 'status': 'idle', 'activities': [], 'client_status': {'mobile': 'idle'}}],
    })
    await asyncio.wait_for(task, 1)
    status = {m.id: m.status for m in guild.members}
    check(status.get(10) == discord.Status.online, "member with online presence cached online")
    check(status.get(11) == discord.Status.idle, "member with idle presence cached idle")
    check(status.get(12) == discord.Status.offline, "member without presence cached offline")

    # ---------- 3. Explicit presences=False still honoured ----------
    await state.chunker(1, presences=False, nonce='x')
    check(ws.requests[-1][1].get('presences') is False, "explicit presences=False passed through")

    # ---------- 4. Incompatible discord.py fails loudly ----------
    class OldState:
        async def chunker(self, guild_id, query='', limit=0, *, nonce=None): pass
    try:
        _chunk_with_presences(OldState())
        check(False, "chunker without 'presences' raises")
    except RuntimeError:
        check(True, "chunker without 'presences' raises")


asyncio.run(main())
print(f"\n{'ALL PASSED' if not failures else f'{len(failures)} FAILED'}")
sys.exit(1 if failures else 0)

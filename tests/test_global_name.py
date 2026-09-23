"""Global display-name changes reach the stored nickname (issue 16)."""
import asyncio, os, sys, json, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
import discord
from database import DatabaseManager
from cogs.tracking import TrackingCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

UID = 42


def user(username, global_name):
    return discord.User(state=None, data={'id': str(UID), 'username': username, 'discriminator': '0',
                                           'avatar': None, 'global_name': global_name})


async def main():
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "g.db"), pool_size=2)
    for gid in (1, 2, 3):
        db.add_guild(gid, f"G{gid}")
    db.add_member(1, UID, "alex", "Alex", 0, [])      # guild 1: no server nickname, display = global name
    db.add_member(2, UID, "alex", "Al", 0, [])        # guild 2: server nickname "Al"
    # guild 3: member not tracked (no row)

    members = {1: SimpleNamespace(nick=None), 2: SimpleNamespace(nick="Al"), 3: SimpleNamespace(nick=None)}
    guilds = [SimpleNamespace(id=gid, get_member=lambda uid, gid=gid: members[gid] if uid == UID else None) for gid in (1, 2, 3)]
    cog = TrackingCog.__new__(TrackingCog)
    cog.db = db
    cog.bot = SimpleNamespace(guilds=guilds, opted_out_users=set())

    def set_display(after):
        for m in members.values():
            m.display_name = m.nick or after.global_name or after.name

    async def change(before, after):
        set_display(after)
        await TrackingCog.on_user_update(cog, before, after)

    row = lambda gid: db.get_member(gid, UID)

    # 1. Global display name Alex -> Alexander
    await change(user("alex", "Alex"), user("alex", "Alexander"))
    check(row(1)['nickname'] == "Alexander", f"no server nick: stored name follows the global name -> {row(1)['nickname']!r}")
    check(json.loads(row(1)['nickname_history']) == ["Alex", "Alexander"], "old and new name in nickname history")
    check(row(2)['nickname'] == "Al", "server nickname unchanged by a global name change")
    check(db.get_member(3, UID) is None, "untracked guild: no row created")

    # 2. Username change only
    await change(user("alex", "Alexander"), user("alex2", "Alexander"))
    check(row(1)['username'] == "alex2" and row(2)['username'] == "alex2", "username updated in every guild")
    check(row(1)['nickname'] == "Alexander", "username change leaves the display name alone")

    # 3. Global name removed -> display name falls back to the username
    await change(user("alex2", "Alexander"), user("alex2", None))
    check(row(1)['nickname'] is None, "global name removed: stored nickname cleared (display = username)")

    # 4. Unrelated profile change (e.g. avatar) and opted-out users do no DB work
    calls = []
    real = db.update_member_nickname
    db.update_member_nickname = lambda *a: calls.append(a) or real(*a)
    await change(user("alex2", None), user("alex2", None))
    cog.bot.opted_out_users.add(UID)
    await change(user("alex2", None), user("alex2", "New"))
    check(not calls, "avatar-only change and opted-out user: no writes")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

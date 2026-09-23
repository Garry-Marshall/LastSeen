"""Verify DM watches pause/resume/delete with the recipient's admin status."""
import asyncio
import os
import sys
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.watch import WatchCog

G, ADMIN, TARGET, OTHER = 1, 10, 20, 30
NOW = int(datetime.now(timezone.utc).timestamp())
failures = []


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


class FakeMember:
    def __init__(self, uid, admin_role=False):
        self.id, self.bot = uid, False
        self.admin_role = admin_role
        self.sent = []
        self.mention = f"<@{uid}>"
    @property
    def roles(self):
        return [SimpleNamespace(name='LastSeen Admin', is_default=lambda: False)] if self.admin_role else []
    @property
    def guild_permissions(self):
        return SimpleNamespace(administrator=False)
    async def send(self, embed=None): self.sent.append(embed)
    def __str__(self): return f"m{self.id}"


class FakeGuild:
    def __init__(self, members):
        self.id, self.name, self.chunked = G, "Guild", True
        self._m = {m.id: m for m in members}
    def get_member(self, uid): return self._m.get(uid)


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "w.db"), pool_size=2)
    db.add_guild(G, "Guild")
    db.add_member(G, TARGET, "target", None, NOW - 999999, [])
    db.update_last_seen(G, TARGET, NOW - 10 * 86400)   # offline 10 days

    admin = FakeMember(ADMIN, admin_role=True)
    guild = FakeGuild([admin, FakeMember(TARGET)])
    bot = SimpleNamespace(no_watch_users=set(), opted_out_users=set(), watch_guild_ids=set(),
                          get_guild=lambda gid: guild if gid == G else None)
    cog = SimpleNamespace(db=db, bot=bot)
    for name in ('_dm_recipient', '_fire_alert', '_sweep_user_watch', '_refresh_watch_guilds'):
        attr = getattr(WatchCog, name)
        setattr(cog, name, attr if isinstance(WatchCog.__dict__[name], staticmethod) else attr.__get__(cog))

    db.add_watch(G, 'user', TARGET, 'offline_for', 7 * 86400, None, ADMIN, deliver_dm=True)
    cog._refresh_watch_guilds()
    watch = lambda: db.get_guild_watches(G)[0]

    # 1. Demoted admin: no DM, stays armed
    admin.admin_role = False
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(admin.sent == [] and watch()['state'] == 'armed', "demoted recipient: no DM, watch stays armed (paused)")

    # 2. Admin restored: fires on next sweep
    admin.admin_role = True
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(len(admin.sent) == 1 and watch()['state'] == 'triggered', "admin restored: DM sent, watch triggered")

    # 3. Recipient not in cache (left / cache loading): paused, not fetched via API
    guild._m.pop(ADMIN)
    db.update_watch_fire_state(watch()['id'], state='armed')
    await cog._sweep_user_watch(guild, watch(), NOW)
    check(watch()['state'] == 'armed', "recipient not a member: paused, no fetch_user fallback")

    # 4. Recipient leaves the guild -> DM watches deleted, channel watch kept
    db.add_watch(G, 'user', TARGET, 'online_return', None, 555, ADMIN, deliver_dm=False)
    await WatchCog.on_member_remove(cog, SimpleNamespace(guild=guild, id=ADMIN))
    kinds = [(w['alert_type'], w['deliver_dm']) for w in db.get_guild_watches(G)]
    check(kinds == [('online_return', 0)], f"leave deletes only the DM watch {kinds}")
    check(G in bot.watch_guild_ids, "guild still indexed (channel watch remains)")

    # 5. /forgetme by a DM recipient deletes their DM watches everywhere
    db.add_watch(G, 'role', 777, 'offline_for', 86400, None, OTHER, deliver_dm=True)
    db.purge_user_data(OTHER)
    check(all(w['created_by'] != OTHER for w in db.get_guild_watches(G)), "/forgetme removes recipient's DM watches")

    # 6. Channel delivery unaffected by the creator's admin status
    chan_sent = []
    channel = SimpleNamespace(id=555, name="c", permissions_for=lambda me: SimpleNamespace(send_messages=True))
    async def send(embed=None): chan_sent.append(embed)
    channel.send = send
    guild.get_channel_or_thread = lambda cid: channel if cid == 555 else None
    guild.me = None
    ok = await cog._fire_alert(guild, db.get_guild_watches(G)[0], "<@20>")
    check(ok and len(chan_sent) == 1, "channel watch from a departed creator still posts")

    # 7. /watch list marker
    db.add_watch(G, 'user', TARGET, 'offline_for', 7 * 86400, None, ADMIN, deliver_dm=True)
    w = [x for x in db.get_guild_watches(G) if x['deliver_dm']][0]
    check(WatchCog._dm_recipient(guild, w, 'LastSeen Admin') is None, "list: departed recipient -> paused")
    guild._m[ADMIN] = admin
    check(WatchCog._dm_recipient(guild, w, 'LastSeen Admin') is admin, "list: admin recipient -> active")

    # 8. Role offline_for sweep: paused keeps fired_targets empty, resumes later
    cog._sweep_role_watch = WatchCog._sweep_role_watch.__get__(cog)
    target_member = guild.get_member(TARGET)
    guild.get_role = lambda rid: SimpleNamespace(id=rid, name="Staff", members=[target_member]) if rid == 888 else None
    db.add_watch(G, 'role', 888, 'offline_for', 7 * 86400, None, ADMIN, deliver_dm=True)
    rw = lambda: [x for x in db.get_guild_watches(G) if x['target_id'] == 888][0]
    admin.admin_role, admin.sent = False, []
    await cog._sweep_role_watch(guild, rw(), NOW)
    check(admin.sent == [] and rw()['fired_targets'] is None, "role watch paused: no DM, fired_targets untouched")
    admin.admin_role = True
    await cog._sweep_role_watch(guild, rw(), NOW)
    check(len(admin.sent) == 1 and rw()['fired_targets'] == f"[{TARGET}]", "role watch resumed: DM sent, member recorded")

    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED")
sys.exit(1 if failures else 0)

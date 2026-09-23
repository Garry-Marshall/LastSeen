"""Role watches on large roles: batched lookup, capped listing, online digest (issue 13)."""
import asyncio, os, sys, json, sqlite3, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.watch import WatchCog, ROLE_DIGEST_SECONDS, ALERT_LIST_MAX

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
G, ROLE, CHANNEL = 1, 777, 555


class FakeRole:
    def __init__(self, rid, members=()): self.id, self.name, self.members = rid, "Staff", list(members)
    def is_default(self): return False


class FakeMember:
    def __init__(self, uid, guild, roles):
        self.id, self.guild, self.roles, self.bot = uid, guild, roles, False
        self.mention = f"<@{uid}>"


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "r.db"), pool_size=2)
    db.add_guild(G, "G")

    # ---------- 1. last-seen lookup for a 40,000-member role ----------
    ids = list(range(1, 40001))
    with db.get_connection() as c:
        c.executemany("INSERT INTO members (guild_id,user_id,username,last_seen,roles) VALUES (1,?,?,?,'[]')",
                      [(u, f"u{u}", NOW - 10 * 86400) for u in ids])
    raw = sqlite3.connect(db.db_file)
    try:
        raw.execute(f"SELECT 1 FROM members WHERE user_id IN ({','.join('?' * len(ids))})", ids)
        old_failed = False
    except sqlite3.OperationalError:
        old_failed = True
    raw.close()
    got = db.get_members_last_seen(G, ids)
    check(old_failed and len(got) == 40000, f"40,000 ids: single statement fails ({old_failed}), batched lookup returns {len(got)}")

    # ---------- fake guild / channel ----------
    sent = []
    channel = SimpleNamespace(id=CHANNEL, name="alerts",
                              permissions_for=lambda me: SimpleNamespace(send_messages=True))
    async def send(embed=None): sent.append(embed)
    channel.send = send
    guild = SimpleNamespace(id=G, name="G", me=None, chunked=True,
                            get_channel_or_thread=lambda cid: channel if cid == CHANNEL else None,
                            get_member=lambda uid: None)
    bot = SimpleNamespace(opted_out_users=set(), no_watch_users=set(), watch_guild_ids={G},
                          get_guild=lambda gid: guild if gid == G else None)
    cog = WatchCog.__new__(WatchCog)
    cog.db, cog.bot = db, bot
    cog._online_cooldown, cog._role_returns, cog._suppress_online_until = {}, {}, 0

    # ---------- 2. offline alert for 3,000 members at once ----------
    role = FakeRole(ROLE)
    role.members = [FakeMember(u, guild, [role]) for u in range(1, 3001)]
    guild.get_role = lambda rid: role if rid == ROLE else None
    db.add_watch(G, 'role', ROLE, 'offline_for', 7 * 86400, CHANNEL, 99)
    w = [x for x in db.get_guild_watches(G) if x['alert_type'] == 'offline_for'][0]
    await cog._sweep_role_watch(guild, w, NOW)
    desc = sent[-1].description if sent else ""
    fired = json.loads(db.get_watch(w['id'])['fired_targets'])
    check(len(sent) == 1 and len(desc) < 4096, f"3,000 offline members: one alert, {len(desc)} chars (< 4096)")
    check(desc.count("• <@") == ALERT_LIST_MAX and "…and 2,950 more" in desc, "lists 50 members + '…and 2,950 more'")
    check(len(fired) == 3000, f"all 3,000 recorded as alerted ({len(fired)})")

    # ---------- 3. online-return digest ----------
    sent.clear()
    db.add_watch(G, 'role', ROLE, 'online_return', None, CHANNEL, 99)
    db.add_watch(G, 'user', 5, 'online_return', None, CHANNEL, 99)
    for u in range(1, 121):
        await cog.on_lastseen_member_online(FakeMember(u, guild, [role]), NOW - 3600)
    check(len(sent) == 1 and "<@5>" in sent[0].description, "user watch still alerts instantly (1 send for member 5)")
    check(len(cog._role_returns) == 1 and len(next(iter(cog._role_returns.values()))['members']) == 120,
          "120 role returns collected into one pending digest")
    await cog.on_lastseen_member_online(FakeMember(7, guild, [role]), NOW - 3600)
    check(len(next(iter(cog._role_returns.values()))['members']) == 120, "repeat return within the cooldown not added twice")

    bot.opted_out_users.add(9)
    since = next(iter(cog._role_returns.values()))['since']   # the digest's own start (not the test's NOW)
    await cog._flush_role_digests(since + ROLE_DIGEST_SECONDS - 5)
    check(len(sent) == 1, "not posted before the 10-minute window ends")
    await cog._flush_role_digests(since + ROLE_DIGEST_SECONDS)
    digest = sent[-1].description if len(sent) == 2 else ""
    check(len(sent) == 2 and digest.count("• <@") == ALERT_LIST_MAX and "…and 69 more" in digest,
          "one digest: 50 listed + '…and 69 more' (member 9 opted out meanwhile)")
    check("<@9>" not in digest and not cog._role_returns, "opted-out member excluded, digest cleared")

    # away suffix when the watch has an 'after' threshold
    sent.clear(); cog._online_cooldown.clear()
    db.add_watch(G, 'role', ROLE, 'online_return', 86400, CHANNEL, 99)   # reconfigure: after 1d
    await cog.on_lastseen_member_online(FakeMember(42, guild, [role]), NOW - 9 * 86400)
    await cog._flush_role_digests(NOW + ROLE_DIGEST_SECONDS + 60)
    check(sent and "<@42> (away 9d)" in sent[-1].description, "away-gated watch shows '(away 9d)'")

    # removed watch -> pending digest dropped
    sent.clear(); cog._online_cooldown.clear()
    await cog.on_lastseen_member_online(FakeMember(43, guild, [role]), NOW - 9 * 86400)
    rw = [x for x in db.get_guild_watches(G) if x['target_type'] == 'role' and x['alert_type'] == 'online_return'][0]
    db.remove_watch(G, rw['id'])
    await cog._flush_role_digests(NOW + ROLE_DIGEST_SECONDS + 60)
    check(not sent and not cog._role_returns, "digest for a removed watch is dropped")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

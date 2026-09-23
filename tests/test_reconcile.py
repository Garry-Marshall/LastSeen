"""Verification for member reconciliation (run from the repo root with the venv python)."""
import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from database import DatabaseManager
from cogs.tracking import (TrackingCog, diff_member_state, _GuildReconcile,
                           MASS_DEPARTURE_MIN, HEARTBEAT_KEY)

G = 1000
NOW = int(datetime.now(timezone.utc).timestamp())
SINCE = NOW - 3600
failures = []


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


class FakeRole:
    def __init__(self, name): self.name = name


class FakeMember:
    def __init__(self, uid, online, name=None, nick=None, bot=False):
        self.id = uid
        self.name = name or f"user{uid}"
        self.display_name = nick or self.name
        self.joined_at = datetime.fromtimestamp(NOW - 86400 * (uid % 50 + 1), tz=timezone.utc)
        self.roles = [FakeRole('@everyone'), FakeRole('Member')]
        self.status = discord.Status.online if online else discord.Status.offline
        self.bot = bot

    def __str__(self): return self.name


# ---------- 1. pure diff ----------
cache = {1: True, 2: False, 3: True, 4: False, 5: True, 6: False}
rows = {
    1: (SINCE - 50, 1),   # online now, stored offline          -> online
    2: (0, 1),            # offline now, stored online           -> offline
    3: (0, 1),            # online, stored online                -> no change
    4: (SINCE - 50, 1),   # offline, stored offline              -> no change
    5: (SINCE - 50, 0),   # stored departed, in guild            -> rejoined
    # 6 missing                                                  -> new
    7: (0, 1),            # stored active, not in guild          -> departed
    8: (SINCE, 0),        # stored departed, not in guild        -> no change
    9: (None, 1),         # never seen, not in guild             -> departed
}
rows[10] = (None, 1); cache[10] = True   # never seen, online now -> online
d = diff_member_state(cache, rows)
check(sorted(d.online) == [1, 10], f"diff online {d.online}")
check(d.offline == [2], f"diff offline {d.offline}")
check(d.rejoined == [5], f"diff rejoined {d.rejoined}")
check(d.new == [6], f"diff new {d.new}")
check(sorted(d.departed) == [7, 9], f"diff departed {d.departed}")

# ---------- 2. DB writes ----------
tmp = tempfile.mkdtemp()
db = DatabaseManager(os.path.join(tmp, "t.db"), pool_size=2)
db.add_guild(G, "TestGuild")
for uid, (ls, active) in rows.items():
    db.add_member(G, uid, f"user{uid}", None, NOW - 86400 * uid, [])
    with db.get_connection() as c:
        c.execute("UPDATE members SET last_seen = ?, is_active = ? WHERE guild_id = ? AND user_id = ?",
                  (ls, active, G, uid))
with db.get_connection() as c:
    c.execute("UPDATE members SET left_date = ? WHERE guild_id = ? AND user_id = 5", (SINCE - 50, G))
db.mark_positions_initialized(G)

check(db.get_member_presence_rows(G) == rows, "get_member_presence_rows returns stored state")

new_row = (6, "user6", "Nick6", NOW - 10, ["Member"], False)
rejoin_row = (5, "user5-renamed", None, NOW - 20, ["Member"], True)
ok = db.reconcile_members(G, d.online, d.offline, d.departed, [rejoin_row], [new_row], SINCE)
check(ok, "reconcile_members returned True")
m = {uid: db.get_member(G, uid) for uid in range(1, 11)}
check(m[1]['last_seen'] == 0 and m[10]['last_seen'] == 0, "online -> last_seen 0")
check(m[2]['last_seen'] == SINCE, "offline -> last_seen = since")
check(m[3]['last_seen'] == 0 and m[4]['last_seen'] == SINCE - 50, "unchanged rows untouched")
check(m[5]['is_active'] == 1 and m[5]['left_date'] == SINCE - 50 and m[5]['last_seen'] == 0
      and m[5]['username'] == "user5-renamed" and m[5]['join_date'] == NOW - 20, "rejoin restored, left_date kept")
# join_dates: 6 -> NOW-10 (newest), 5 -> NOW-20 (second newest) -> positions 10 and 9
check(m[5]['join_position'] == 9, f"rejoiner join_position recomputed {m[5]['join_position']}")
check(m[6] is not None and m[6]['is_active'] == 1 and m[6]['last_seen'] is None
      and m[6]['nickname'] == "Nick6" and json.loads(m[6]['nickname_history']) == ["Nick6"], "new member inserted")
# join_date NOW-10 is the newest of all 10 rows -> position 10
check(m[6]['join_position'] == 10, f"new member join_position {m[6]['join_position']}")
check(m[7]['is_active'] == 0 and m[7]['left_date'] == SINCE and m[7]['last_seen'] == SINCE, "departed (was online)")
check(m[9]['is_active'] == 0 and m[9]['left_date'] == SINCE, "departed (never seen)")
check(m[8]['left_date'] is None and m[8]['last_seen'] == SINCE, "already-departed row untouched")

# idempotent: re-running changes nothing
before = db.get_member_presence_rows(G)
d2 = diff_member_state(cache, before)
check(not any(d2), f"second diff is empty {d2}")

# batching across >500 rows
many = list(range(100000, 101200))
for uid in many:
    db.add_member(G, uid, f"u{uid}", None, NOW, [])
ok = db.reconcile_members(G, many, [], [], [], [], SINCE)
check(ok and all(db.get_member_presence_rows(G)[u][0] == 0 for u in many), "1200-row batch applied")

# ---------- 3. heartbeat ----------
check(db.get_bot_state(HEARTBEAT_KEY) is None, "heartbeat unset initially")
db.set_bot_state(HEARTBEAT_KEY, 123); db.set_bot_state(HEARTBEAT_KEY, 456)
check(db.get_bot_state(HEARTBEAT_KEY) == 456, "heartbeat upsert")

# ---------- 4. _reconcile_guild guards ----------
G2 = 2000
db.add_guild(G2, "Guard")
for uid in range(1, 201):
    db.add_member(G2, uid, f"g{uid}", None, NOW - 1000, [])
fake = SimpleNamespace(db=db, bot=SimpleNamespace(opted_out_users=set()),
                       _member_row=TrackingCog._member_row)
# Cache shows only 100 of 200 -> 100 departures (> 50 and > 20%) -> skipped
snap = {uid: False for uid in range(1, 101)}
TrackingCog._reconcile_guild(fake, _GuildReconcile(G2, "Guard", snap, {}, SINCE))
active = sum(1 for _, a in db.get_member_presence_rows(G2).values() if a)
check(active == 200, f"mass departure skipped (active={active})")
# 10 missing -> applied
snap = {uid: False for uid in range(1, 191)}
TrackingCog._reconcile_guild(fake, _GuildReconcile(G2, "Guard", snap, {}, SINCE))
active = sum(1 for _, a in db.get_member_presence_rows(G2).values() if a)
check(active == 190, f"small departure applied (active={active})")
# opted out after snapshot -> not inserted
fake.bot.opted_out_users = {500}
TrackingCog._reconcile_guild(fake, _GuildReconcile(
    G2, "Guard", {**snap, 500: True, 501: True}, {500: FakeMember(500, True), 501: FakeMember(501, True)}, SINCE))
r = db.get_member_presence_rows(G2)
check(500 not in r and r.get(501) == (0, 1), "opted-out skipped, new online member inserted")
# unregistered guild -> no-op, no exception
TrackingCog._reconcile_guild(fake, _GuildReconcile(9999, "Nope", {1: True}, {1: FakeMember(1, True)}, SINCE))
check(db.get_member_presence_rows(9999) == {}, "unregistered guild ignored")


# ---------- 5. live rejoin + leave/join counting ----------
D = 86400
G3 = 3000
db.add_guild(G3, "Counts")
with db.get_connection() as c:
    c.execute("UPDATE guilds SET added_at = ? WHERE guild_id = ?", (NOW - 400 * D, G3))


def seed(uid, join, active, left=None, last_seen=None):
    db.add_member(G3, uid, f"c{uid}", None, join, [])
    with db.get_connection() as c:
        c.execute("UPDATE members SET is_active = ?, left_date = ?, last_seen = ? WHERE guild_id = ? AND user_id = ?",
                  (active, left, last_seen, G3, uid))

seed(1, NOW - 200 * D, 0, left=NOW - 3 * D, last_seen=NOW - 3 * D)      # A: left in window, still gone
seed(2, NOW - 200 * D, 0, left=NOW - 3 * D, last_seen=NOW - 3 * D)      # B: left in window, rejoins below
seed(3, NOW - 200 * D, 0, left=NOW - 90 * D, last_seen=NOW - 90 * D)    # C: left before window, rejoins below
seed(4, NOW - 200 * D, 0, left=None, last_seen=NOW - 3 * D)             # D: legacy departure (no left_date)
seed(5, NOW - 200 * D, 1, left=None, last_seen=NOW - 3 * D)             # E: long-time member, untouched
db.mark_positions_initialized(G3)

for uid in (2, 3):
    check(db.rejoin_member(G3, uid, f"c{uid}-back", "Back", NOW - 2 * D, ["Member"], True), f"rejoin_member({uid}) ok")
r2 = db.get_member(G3, 2)
check(r2['is_active'] == 1 and r2['left_date'] == NOW - 3 * D and r2['join_date'] == NOW - 2 * D
      and r2['last_seen'] == 0 and r2['nickname'] == "Back" and r2['roles'] == ["Member"], "live rejoin fields")
check(r2['join_position'] == 5, f"live rejoin position (newest of 5) {r2['join_position']}")
check(db.rejoin_member(G3, 5, "c5", None, NOW - 200 * D, [], False) and db.get_member(G3, 5)['last_seen'] == NOW - 3 * D,
      "offline rejoin keeps last_seen")

growth = db.get_member_growth_stats(G3, 30)
check(growth['joins'] == 2, f"growth joins = B + C ({growth['joins']})")
check(growth['leaves'] == 3, f"growth leaves = A + B + legacy D ({growth['leaves']})")
check(growth['net_growth'] == -1, f"growth net: +2 joins -3 leaves ({growth['net_growth']})")
health = db.get_server_health(G3)
check(health['leaves_30d'] == 3 and health['joins_30d'] == 2, f"pulse leaves/joins {health['leaves_30d']}/{health['joins_30d']}")
departed = sorted(m['user_id'] for m in db.get_departed_members_period(G3, 7))
check(departed == [1, 2, 4], f"report departed includes rejoiner B and legacy D {departed}")
new = sorted(m['user_id'] for m in db.get_new_members_period(G3, 7))
check(new == [2, 3], f"report joined includes rejoiners {new}")
snap = db.get_server_snapshot_stats(G3)
month_start = int(datetime(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1, tzinfo=timezone.utc).timestamp())
exp_leaves = 3 if NOW - 3 * D >= month_start else 0
check(snap['leaves_this_month'] == exp_leaves, f"snapshot leaves this month {snap['leaves_this_month']} (expected {exp_leaves})")
db.record_health_snapshots()
with db.get_connection() as c:
    row = c.execute("SELECT joins_7d, leaves_7d FROM health_snapshots WHERE guild_id = ?", (G3,)).fetchone()
check(tuple(row) == (2, 3), f"nightly health snapshot joins/leaves {tuple(row)}")
life = db.get_departure_lifespan(G3)
check(life['sample'] == 1, f"lifespan counts only members still gone with left_date (A) -> {life['sample']}")


# Issue 7: join then leave inside one report window -> 1 joined, 1 left, net 0
G4 = 4000
db.add_guild(G4, "Churn")
db.add_member(G4, 1, "churner", None, NOW - 5 * D, [])      # joined 5 days ago...
with db.get_connection() as c:                               # ...left 4 days ago
    c.execute("UPDATE members SET is_active = 0, left_date = ?, last_seen = ? WHERE guild_id = ? AND user_id = 1",
              (NOW - 4 * D, NOW - 4 * D, G4))
db.add_member(G4, 2, "stayer", None, NOW - 5 * D, [])       # joined, still here
joined = len(db.get_new_members_period(G4, 7))
left = len(db.get_departed_members_period(G4, 7))
check((joined, left, joined - left) == (2, 1, 1), f"report join-then-leave: joined {joined}, left {left}, net {joined - left}")


# ---------- 6. snapshot + since selection ----------
async def snapshot_tests():
    guild = SimpleNamespace(id=G, name="TestGuild", chunked=True,
                            members=[FakeMember(1, True), FakeMember(2, False),
                                     FakeMember(3, True, bot=True), FakeMember(4, True)])
    cog = SimpleNamespace(bot=SimpleNamespace(opted_out_users={4}), _reconcile_queued=set(),
                          _stale_since={}, _reconciled_guilds=set(), _startup_stale_since=SINCE,
                          _presence_queue=asyncio.Queue())
    await TrackingCog.on_lastseen_guild_chunked(cog, guild)
    item = cog._presence_queue.get_nowait()
    check(item.online == {1: True, 2: False}, f"snapshot skips bots/opted-out {item.online}")
    check(item.since == SINCE, "first reconcile uses startup heartbeat")

    # Duplicate while queued -> ignored
    await TrackingCog.on_lastseen_guild_chunked(cog, guild)
    check(cog._presence_queue.empty(), "duplicate reconcile while queued is skipped")

    # Later reconcile after a shard disconnect uses the disconnect time, not startup
    cog._reconcile_queued.clear()
    cog._stale_since[G] = NOW - 60
    await TrackingCog.on_lastseen_guild_chunked(cog, guild)
    check(cog._presence_queue.get_nowait().since == NOW - 60, "reconnect uses disconnect time")

    # Neither -> now
    cog._reconcile_queued.clear()
    await TrackingCog.on_lastseen_guild_chunked(cog, guild)
    check(abs(cog._presence_queue.get_nowait().since - NOW) < 5, "no record -> now")

    # Incomplete cache -> nothing queued
    cog._reconcile_queued.clear()
    guild.chunked = False
    await TrackingCog.on_lastseen_guild_chunked(cog, guild)
    check(cog._presence_queue.empty(), "unchunked guild not reconciled")

asyncio.run(snapshot_tests())

db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED")
sys.exit(1 if failures else 0)

"""Issue 17: DB work moved off the event loop, behaviour unchanged."""
import asyncio, os, sys, tempfile, threading, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.locale import load_locales
from bot.utils import format_timestamp
from database import DatabaseManager
from cogs.tracking import TrackingCog
from cogs.admin.channel_config import ChannelModal, InactiveDaysModal, TimezoneModal, ReportsConfigModal, RetentionDaysModal
from cogs.admin.role_config import BotAdminRoleModal, UserRoleModal, TrackOnlyRolesModal
from cogs.admin.channel_filter import AllowedChannelsModal
from cogs.admin.config_view import ConfigView, DisableReportsConfirmView, LanguageSelectView
from cogs.admin.quick_setup import QuickSetupView

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

NOW = int(datetime.now(timezone.utc).timestamp())
G = 1
MAIN = threading.main_thread()


class RecordingDB:
    """Wraps DatabaseManager; records the thread every call runs on."""
    def __init__(self, db):
        self._db, self.calls = db, []
    def __getattr__(self, name):
        attr = getattr(self._db, name)
        if not callable(attr):
            return attr
        def wrapped(*a, **kw):
            self.calls.append((name, threading.current_thread() is MAIN))
            return attr(*a, **kw)
        return wrapped
    def on_loop(self):
        return [n for n, on_main in self.calls if on_main]


class FakeRole:
    def __init__(self, name): self.name, self.id = name, hash(name) % 10000
    def is_default(self): return self.name == '@everyone'


class FakeMember:
    def __init__(self, uid, guild, nick=None, roles=('@everyone',), online=True):
        self.id, self.guild, self.nick, self.bot = uid, guild, nick, False
        self.name = f"user{uid}"
        self.display_name = nick or self.name
        self.roles = [FakeRole(r) for r in roles]
        self.top_role = self.roles[-1]
        self.joined_at = datetime.fromtimestamp(NOW - 3600, tz=timezone.utc)
        self.status = discord.Status.online if online else discord.Status.offline
        self.display_avatar = SimpleNamespace(url="https://x")
    def __str__(self): return self.name


async def main():
    load_locales()

    # ---------- format_timestamp: no DB, same output ----------
    check(format_timestamp(NOW, 'R') == f"<t:{NOW}:R>" and format_timestamp(None) == "Never",
          "format_timestamp renders without any DB access")

    # ---------- tracking event handlers ----------
    real = DatabaseManager(os.path.join(tempfile.mkdtemp(), "o.db"), pool_size=2)
    real.add_guild(G, "G")
    real.mark_positions_initialized(G)
    real.add_member(G, 1, "user1", None, NOW - 999999, [])
    db = RecordingDB(real)
    guild = SimpleNamespace(id=G, name="G")
    cog = TrackingCog.__new__(TrackingCog)
    cog.db, cog.config = db, SimpleNamespace(default_inactive_days=10)
    cog.bot = SimpleNamespace(opted_out_users=set(), get_channel=lambda cid: None)
    cog._member_locks = {}

    await TrackingCog.on_member_join(cog, FakeMember(2, guild, online=True))
    m2 = real.get_member(G, 2)
    check(m2 and m2['last_seen'] == 0 and m2['join_position'] == 2, f"new member: stored, online, position 2 -> {m2 and m2['join_position']}")
    await TrackingCog.on_member_remove(cog, FakeMember(2, guild))
    m2 = real.get_member(G, 2)
    check(m2['is_active'] == 0 and m2['left_date'], "leave recorded")
    await TrackingCog.on_member_join(cog, FakeMember(2, guild, online=False))
    check(real.get_member(G, 2)['is_active'] == 1, "rejoin recorded")
    before = FakeMember(1, guild, roles=('@everyone',))
    after = FakeMember(1, guild, nick="Nick", roles=('@everyone', 'Mod'))
    await TrackingCog.on_member_update(cog, before, after)
    m1 = real.get_member(G, 1)
    hist = real.get_role_history(G, 1)
    check(m1['nickname'] == "Nick" and m1['roles'] == ['Mod'] and hist and hist[0]['role_name'] == 'Mod',
          "member update: nickname, roles and role history stored")
    await TrackingCog.on_guild_update(cog, SimpleNamespace(name="G"), SimpleNamespace(id=G, name="Renamed"))
    check(real.get_guild_config(G)['guild_name'] == "Renamed", "guild rename stored")
    check(db.calls and not db.on_loop(), f"all {len(db.calls)} handler DB calls ran off the event loop "
                                          f"(on loop: {db.on_loop()})")

    # ---------- admin dialogs: built from a passed-in config, no DB reads ----------
    cfg = real.get_guild_config(G)
    built = 0
    for cls in (ChannelModal, InactiveDaysModal, TimezoneModal, ReportsConfigModal, RetentionDaysModal,
                BotAdminRoleModal, UserRoleModal, TrackOnlyRolesModal, AllowedChannelsModal,
                DisableReportsConfirmView, LanguageSelectView):
        for c in (cfg, None):
            cls(None, G, c)   # db=None: any DB access would raise
            built += 1
    ConfigView(None, G, None, cfg); ConfigView(None, G, None, None)
    check(built == 22, "all 11 modals/views + ConfigView build from a passed config with db=None")

    # ---------- quick setup: steps render from the snapshot; refresh is off-loop ----------
    db.calls.clear()
    wiz = QuickSetupView(db, G, None, cfg)
    for step in range(5):
        wiz.current_step = step
        wiz._get_step_embed()
    check(not db.calls, "quick setup renders all 5 steps with no DB calls")
    real.set_inactive_days(G, 21)
    await wiz._refresh_config()
    wiz.current_step = 1
    check("21" in wiz._get_step_embed().description and not db.on_loop(),
          "quick setup picks up a changed setting after refresh (read off-loop)")
    real.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

"""@everyone can't grant bot admin (issue 28)."""
import asyncio, os, sys, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from bot.locale import load_locales
from bot.utils import has_bot_admin_role, can_use_bot_commands
from database import DatabaseManager
from cogs.admin.role_config import BotAdminRoleModal, UserRoleModal

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)


class Role:
    def __init__(self, name, default=False): self.name, self._default = name, default
    def is_default(self): return self._default

EVERYONE = Role("@everyone", default=True)
def member(*names, administrator=False):
    return SimpleNamespace(roles=[EVERYONE] + [Role(n) for n in names],
                           guild_permissions=SimpleNamespace(administrator=administrator))

plain, mod, server_admin = member(), member("Mods"), member(administrator=True)

check(not has_bot_admin_role(plain, "@everyone"), "admin role '@everyone': ordinary member is NOT a bot admin (was)")
check(has_bot_admin_role(mod, "Mods") and not has_bot_admin_role(plain, "Mods"), "named admin role works as before")
check(has_bot_admin_role(server_admin, "@everyone"), "Administrator permission still always counts")
check(has_bot_admin_role(member("everyone"), "everyone"), "a real role named 'everyone' (no @) is unaffected")

cfg = lambda **kw: {'bot_admin_role_name': 'LastSeen Admin', 'user_role_required': 1, 'user_role_name': 'LastSeen User', **kw}
check(can_use_bot_commands(plain, cfg(user_role_name='@everyone')),
      "user role '@everyone' still means everyone may use the commands (unchanged)")
check(not can_use_bot_commands(plain, cfg(bot_admin_role_name='@everyone')),
      "admin role '@everyone' no longer lets everyone past the user-role requirement")
check(can_use_bot_commands(member("LastSeen User"), cfg()) and can_use_bot_commands(plain, cfg(user_role_required=0)),
      "normal user-role behaviour unchanged")


async def submit(modal_cls, value, db):
    sent = []
    async def send_message(*a, **kw): sent.append(kw)
    modal = modal_cls(db, 1, db.get_guild_config(1))
    modal.role_input._value = value
    guild = SimpleNamespace(name="G", default_role=EVERYONE, roles=[EVERYONE, Role("Mods")])
    await modal.on_submit(SimpleNamespace(guild=guild, response=SimpleNamespace(send_message=send_message)))
    return sent[-1]

async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "r.db"), pool_size=2)
    db.add_guild(1, "G")
    for value in ("@everyone", "@Everyone"):
        reply = await submit(BotAdminRoleModal, value, db)
        check(reply.get('ephemeral') and "every member has it" in reply['embed'].description
              and db.get_guild_config(1)['bot_admin_role_name'] == 'LastSeen Admin',
              f"/config refuses admin role {value!r}, setting unchanged")
    await submit(BotAdminRoleModal, "Mods", db)
    check(db.get_guild_config(1)['bot_admin_role_name'] == 'Mods', "a normal admin role is still saved")
    await submit(UserRoleModal, "@everyone", db)
    check(db.get_guild_config(1)['user_role_name'] == '@everyone', "user role '@everyone' can still be saved")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

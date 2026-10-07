"""A new watch on a member asks the admin to confirm the target DM before creating it."""
import asyncio, os, sys, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
import discord
from bot.locale import load_locales, t
from database import DatabaseManager
from cogs.watch import WatchCog, WatchConfirmView

G, ADMIN, TARGET, ROLE, CHANNEL = 1, 10, 20, 777, 555
failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)


class FakeMember:
    def __init__(self, uid):
        self.id, self.bot, self.sent = uid, False, []
    async def send(self, embed=None): self.sent.append(embed)
    def __str__(self): return f"m{self.id}"


class FakeInteraction:
    """Covers both the slash command (send_message) and a button press (defer/edit)."""
    def __init__(self, guild, channel, user):
        self.guild, self.guild_id, self.channel, self.user = guild, guild.id, channel, user
        self.sent, self.edits = [], []
        self.response = SimpleNamespace(send_message=self._send, defer=self._defer, edit_message=self._edit)
    async def _send(self, embed=None, view=None, ephemeral=False): self.sent.append((embed, view))
    async def _defer(self): pass
    async def _edit(self, embed=None, view=None): self.edits.append(embed)
    async def edit_original_response(self, embed=None, view=None): self.edits.append(embed)


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "c.db"), pool_size=2)
    db.add_guild(G, "G")
    target = FakeMember(TARGET)
    admin = FakeMember(ADMIN)
    channel = SimpleNamespace(id=CHANNEL, permissions_for=lambda me: SimpleNamespace(send_messages=True))
    role = SimpleNamespace(id=ROLE, is_bot_managed=lambda: False)
    members = {TARGET: target, ADMIN: admin}
    guild = SimpleNamespace(id=G, name="G", me=None, get_member=members.get,
                            get_role=lambda rid: role if rid == ROLE else None)
    bot = SimpleNamespace(opted_out_users=set(), no_watch_users=set(), watch_guild_ids=set(),
                          get_user=lambda uid: None)
    cog = WatchCog.__new__(WatchCog)
    cog.db, cog.bot, cog._online_cooldown = db, bot, {}
    user_target = discord.Object(id=TARGET)
    cancelled = t('watch.confirm_cancelled', 'en')

    def watches(): return db.get_guild_watches(G)
    async def prompt():
        inter = FakeInteraction(guild, channel, admin)
        await cog._add_watch(inter, 'en', user_target, 'online_return', None, None)
        return inter.sent[-1]

    # 1. New user watch: confirmation prompt, nothing saved, no DM yet.
    embed, view = await prompt()
    check(isinstance(view, WatchConfirmView), "new user watch shows a Proceed/Cancel prompt")
    check("<@20>" in embed.description, "prompt names the member who will be DM'd")
    check(not watches() and not target.sent, "nothing saved and no DM before confirming")

    # 2. Cancel: still nothing saved, no DM.
    press = FakeInteraction(guild, channel, admin)
    await view.cancel_button.callback(press)
    check(press.edits and press.edits[-1].description == cancelled, "cancel shows 'no watch created'")
    check(not watches() and not target.sent, "cancel: no watch, no DM")

    # 3. Timeout behaves like cancel.
    embed, view = await prompt()
    await view.on_timeout()
    check(view.interaction.edits and view.interaction.edits[-1].description == cancelled, "timeout shows 'no watch created'")
    check(not watches() and not target.sent, "timeout: no watch, no DM")

    # 4. Proceed: watch saved, DM sent once, confirmation shown.
    embed, view = await prompt()
    press = FakeInteraction(guild, channel, admin)
    await view.proceed_button.callback(press)
    check(len(watches()) == 1, "proceed saves the watch")
    check(len(target.sent) == 1, "proceed DMs the member once")
    check(press.edits and press.edits[-1].title == t('watch.added_title', 'en'), "proceed shows the 'Watch created' confirmation")
    check(bot.watch_guild_ids == {G}, "proceed refreshes the watch guild gate")

    # 5. Reconfiguring an existing user watch: no prompt, no new DM.
    inter = FakeInteraction(guild, channel, admin)
    await cog._add_watch(inter, 'en', user_target, 'online_return', 7 * 86400, None)
    check(inter.sent[-1][1] is None and inter.sent[-1][0].title == t('watch.added_title', 'en'),
          "reconfigure saves directly without a prompt")
    check(len(target.sent) == 1, "reconfigure sends no DM")

    # 6. Role watch: no prompt (roles aren't DM'd).
    inter = FakeInteraction(guild, channel, admin)
    role_target = discord.Role.__new__(discord.Role)
    role_target.id = ROLE
    await cog._add_watch(inter, 'en', role_target, 'online_return', None, None)
    check(inter.sent[-1][1] is None and len(watches()) == 2, "role watch saves directly without a prompt")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

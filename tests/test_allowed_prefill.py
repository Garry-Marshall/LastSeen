"""Set Allowed Channels shows the current setting, and saving it unchanged keeps it (fix)."""
import asyncio, os, sys, json, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.admin.channel_filter import AllowedChannelsModal

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)


class FakeText(discord.TextChannel):
    """Passes isinstance(…, discord.TextChannel) like a real text channel."""
    def __init__(self, cid, name):
        self.id, self.name = cid, name
    @property
    def mention(self): return f"<#{self.id}>"


def forum(cid, name):
    return SimpleNamespace(id=cid, name=name, mention=f"<#{cid}>")


general, bots, dup_a, dup_b = FakeText(1, "general"), FakeText(2, "bot-commands"), FakeText(3, "chat"), FakeText(4, "chat")
forum_ch = forum(5, "help-forum")
channels = {c.id: c for c in (general, bots, dup_a, dup_b, forum_ch)}
guild = SimpleNamespace(name="G", text_channels=[general, bots, dup_a, dup_b],
                        get_channel=lambda cid: channels.get(cid))


async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "a.db"), pool_size=2)
    db.add_guild(1, "G")

    modal = AllowedChannelsModal(db, 1, db.get_guild_config(1), guild)
    check(not modal.channels_input.default, "no restriction set: field starts empty")

    stored = [1, 2, 3, 5, 999]   # unique text, unique text, duplicate-named text, forum, deleted channel
    db.set_allowed_channels(1, stored)
    modal = AllowedChannelsModal(db, 1, db.get_guild_config(1), guild)
    prefill = modal.channels_input.default
    check(prefill == "#general, #bot-commands, 3, 5, 999",
          f"prefilled: names where unique, IDs otherwise -> {prefill!r}")

    # Save the prefilled text unchanged: the same live channels must come back
    sent = []
    async def send_message(*a, **kw): sent.append(kw)
    modal.channels_input._value = prefill
    await modal.on_submit(SimpleNamespace(guild=guild, response=SimpleNamespace(send_message=send_message)))
    saved = json.loads(db.get_guild_config(1)['allowed_channels'])
    check(saved == [1, 2, 3, 5], f"saving unchanged keeps every existing channel (deleted one dropped) -> {saved}")

    check(modal.channels_input.max_length == 4000, "field limit raised so a long list can't block the dialog")
    long_ids = list(range(10**17, 10**17 + 150))   # 150 channel IDs ~ 3,000 chars
    db.set_allowed_channels(1, long_ids)
    modal = AllowedChannelsModal(db, 1, db.get_guild_config(1), guild)
    check(len(modal.channels_input.default) <= 4000, f"150 channels prefill fits ({len(modal.channels_input.default)} chars)")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

"""View Config never exceeds Discord's 1024-char field limit (issue 30)."""
import asyncio, os, re, sys, tempfile, logging
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
from bot.locale import load_locales
from bot.utils import join_capped
from database import DatabaseManager
from cogs.admin.config_view import ConfigView

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

load_locales()
mentions = [f"<#{100000000000000000 + i}>" for i in range(80)]           # 80 channel mentions
roles = [f"Role {i:02d} " + "x" * 90 for i in range(20)]                  # 20 near-maximum role names

check(join_capped(["a", "b"]) == "a, b", "short list unchanged")
for lang in ('en', 'es', 'fr', 'nl', 'pl'):
    for items in (mentions, roles):
        out = join_capped(items, lang)
        shown = out.count(", ") if items is mentions else sum(1 for r in items if r in out)
        m = re.search(r"(\d[\d,.  ]*)", out.split(", ")[-1])
        more = int(re.sub(r"\D", "", m.group(1))) if m else 0
        ok = len(out) <= 1024 and shown + more == len(items)
        if not ok:
            print("   ", lang, len(out), shown, more, out[-60:])
        check(ok, f"{lang}: {len(items)} items -> {len(out)} chars, listed + 'more' count adds up")

async def main():
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "v.db"), pool_size=2)
    db.add_guild(1, "G")
    db.set_allowed_channels(1, [100000000000000000 + i for i in range(80)])
    db.set_track_only_roles(1, roles)
    sent = []
    async def send_message(*a, **kw): sent.append(kw)
    channel = lambda cid: SimpleNamespace(mention=f"<#{cid}>")
    interaction = SimpleNamespace(guild=SimpleNamespace(get_channel=channel),
                                  response=SimpleNamespace(send_message=send_message))
    view = ConfigView(db, 1, None, db.get_guild_config(1))
    await view.view_config.callback(interaction)
    embed = sent[-1]['embed']
    longest = max(len(f.value) for f in embed.fields)
    check(longest <= 1024 and len(embed) <= 6000,
          f"View Config embed: longest field {longest} chars, total {len(embed)} (limits 1024 / 6000)")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

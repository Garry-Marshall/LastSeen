"""/search sorts before capping, online first (issue 31)."""
import asyncio, os, sys, random, tempfile, logging
from datetime import datetime, timezone
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.CRITICAL)
import discord
from bot.locale import load_locales
from database import DatabaseManager
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

random.seed(2)
NOW = int(datetime.now(timezone.utc).timestamp())
G = 1

async def main():
    load_locales()
    db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "o.db"), pool_size=2)
    db.add_guild(G, "G")
    # 1500 members: 200 online, 200 never seen, 1100 offline at random times; insertion order shuffled
    rows, kind = [], {}
    for uid in range(1, 1501):
        if uid <= 200: ls, k = 0, 'online'
        elif uid <= 400: ls, k = None, 'never'
        else: ls, k = NOW - random.randint(60, 90 * 86400), 'offline'
        rows.append((uid, ls)); kind[uid] = k
    random.shuffle(rows)
    with db.get_connection() as c:
        c.executemany("INSERT INTO members (guild_id,user_id,username,join_date,last_seen,roles) VALUES (1,?,?,0,?,'[]')",
                      [(u, f"u{u}", ls) for u, ls in rows])
    sent = []
    async def defer(**kw): pass
    async def send(*a, **kw): sent.append({'content': a[0] if a else None, **kw})
    interaction = SimpleNamespace(
        guild_id=G, guild=SimpleNamespace(id=G, members=[], roles=[], get_role=lambda r: None),
        channel_id=5, channel=None,
        user=SimpleNamespace(id=9, guild_permissions=SimpleNamespace(administrator=True), roles=[]),
        response=SimpleNamespace(defer=defer, send_message=send), followup=SimpleNamespace(send=send))
    await CommandsCog.search.callback(CommandsCog(None, db, None), interaction, export="none")
    results = sent[-1]['view'].results
    ids = [r['user_id'] for r in results]
    kinds = [kind[u] for u in ids]
    check(len(results) == 1000 and any("Showing first 1000" in (k.get('content') or '') or "1000" in str(k) for k in sent[:-1]),
          "1500 matches -> capped at 1000 with the notice")
    check(kinds[:200] == ['online'] * 200, "all 200 online members come first (they were at the very bottom)")
    offline_ts = [r['last_seen'] for r in results[200:]]
    check(all(kind[u] == 'offline' for u in ids[200:]) and offline_ts == sorted(offline_ts, reverse=True),
          "then the 800 most recently offline, newest first")
    kept_offline = set(ids[200:])
    best_offline = sorted((u for u in kind if kind[u] == 'offline'), key=lambda u: dict(rows)[u], reverse=True)[:800]
    check(kept_offline == set(best_offline), "the kept offline members are exactly the 800 most recent (not arbitrary)")
    check('never' not in kinds, "never-seen members are the ones cut (they sort last)")
    db.close_pool()

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

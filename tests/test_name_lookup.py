"""Name lookup: unambiguous precedence + Unicode case-insensitivity (issue 11)."""
import os, sys, tempfile, logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from database import DatabaseManager

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

db = DatabaseManager(os.path.join(tempfile.mkdtemp(), "n.db"), pool_size=2)
G = 1
db.add_guild(G, "G")

def add(uid, username, nickname=None, active=1, last_seen=None):
    db.add_member(G, uid, username, nickname, 0, [])
    with db.get_connection() as c:
        c.execute("UPDATE members SET is_active = ?, last_seen = ? WHERE guild_id = ? AND user_id = ?",
                  (active, last_seen, G, uid))

find = lambda term: (db.find_member_by_name(G, term) or {}).get('user_id')

# Precedence (rows inserted in the "wrong" order on purpose)
add(2, "someone", "Alex")            # nickname Alex
add(1, "alex")                       # username alex
check(find("alex") == 1 and find("ALEX") == 1, "username beats a nickname with the same text")
add(3, "digits", "12345")            # nickname that looks like an id
add(12345, "realid")
check(find("12345") == 12345, "user ID beats a nickname made of the same digits")
add(4, "gone", "Sam", active=0, last_seen=100)
add(5, "here", "Sam", active=1, last_seen=50)
check(find("sam") == 5, "current member beats a departed one with the same nickname")
add(6, "a1", "Kim", last_seen=1000)
add(7, "a2", "Kim", last_seen=0)
add(8, "a3", "Kim", last_seen=2000)
check(find("kim") == 7, "among current members: online now wins")
with db.get_connection() as c:
    c.execute("UPDATE members SET last_seen = 3000 WHERE guild_id = ? AND user_id = 7", (G,))
check(find("kim") == 7, "then the most recently seen (3000 > 2000 > 1000)")

# Unicode case-insensitivity (LOWER() only folded ASCII)
add(20, "Łukasz")
add(21, "emile", "ÉMILE")
add(22, "gunther", "Straße")
add(23, "zoe", "Zoë")
check(find("łukasz") == 20 and find("ŁUKASZ") == 20, "Łukasz found as łukasz / ŁUKASZ (failed before)")
check(find("émile") == 21, "nickname ÉMILE found as émile (failed before)")
check(find("strasse") == 22 and find("STRASSE") == 22, "Straße found as strasse (casefold)")
check(find("ZOË") == 23, "Zoë found as ZOË")
check(find("nobody") is None, "no match -> None")

# Autocomplete substring search
names = lambda q: sorted(r['username'] for r in db.search_members_by_name(G, q))
check(names("łuk") == ["Łukasz"] and names("ŁUK") == ["Łukasz"], "autocomplete: 'łuk' / 'ŁUK' find Łukasz (failed before)")
check(names("ukas") == ["Łukasz"], "autocomplete: ASCII part of a non-ASCII name")
check(names("émi") == ["emile"] and names("STRAS") == ["gunther"], "autocomplete: accented and folded nickname substrings")
check(names("ALE") == ["alex", "someone"], "autocomplete: ASCII names unchanged")
check("gone" not in names("gon"), "autocomplete: departed members still excluded")
add(30, "per%cent"); add(31, "under_score"); add(32, "perXcent")
check(names("%") == ["per%cent"] and names("r_s") == ["under_score"], "autocomplete: % and _ stay literal")

# Connections created when the pool is exhausted also have CASEFOLD
extra = db._create_connection()
check(extra.execute("SELECT CASEFOLD('ÉÉ')").fetchone()[0] == "éé", "overflow connections register CASEFOLD too")
extra.close()
db.close_pool()
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

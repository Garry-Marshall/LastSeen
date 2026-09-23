"""/search inactive / joined / departed filters (issue 10)."""
import os, sys, logging
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))); logging.disable(logging.WARNING)
from bot.locale import load_locales
from cogs.commands import CommandsCog

failures = []
def check(c, m):
    print(("PASS " if c else "FAIL ") + m)
    if not c: failures.append(m)

load_locales()
cog = CommandsCog(None, None, None)
D = 86400
NOW = int(datetime.now(timezone.utc).timestamp())

def f(**kw):
    return cog._parse_search_filters(roles=None, status=None, inactive=kw.get('inactive'), activity=None,
                                     joined=kw.get('joined'), departed=kw.get('departed'),
                                     username=None, guild=None)

def m(last_seen=None, join_date=None, is_active=1, left_date=None):
    return {'username': 'u', 'nickname': None, 'last_seen': last_seen, 'join_date': join_date,
            'is_active': is_active, 'left_date': left_date}

match = lambda member, flt, added_at=0: cog._matches_db_filters(member, flt, added_at)

# inactive: whole days
seen_14_5 = m(last_seen=NOW - int(14.5 * D))
check(match(seen_14_5, f(inactive='=14')), "inactive:=14 matches 14.5 days (never matched before)")
check(not match(seen_14_5, f(inactive='=15')), "inactive:=15 doesn't match 14.5 days")
check(match(seen_14_5, f(inactive='>13')) and not match(seen_14_5, f(inactive='>14')), "inactive:>14 means 15+ full days")
check(match(seen_14_5, f(inactive='<15')) and not match(seen_14_5, f(inactive='<14')), "inactive:<15 includes 14 full days")
check(match(m(last_seen=0), f(inactive='=0')) and not match(m(last_seen=0), f(inactive='>0')), "online now = 0 days")

# inactive: never seen -> days since max(join, bot arrival), like /inactive
never_old = m(last_seen=None, join_date=NOW - 300 * D)
check(match(never_old, f(inactive='>30'), added_at=NOW - 400 * D), "never seen, joined 300d ago: matches inactive:>30 (was 0 days)")
check(not match(never_old, f(inactive='>30'), added_at=NOW - 10 * D), "never seen, bot added 10d ago: 10 days, not >30")
check(match(never_old, f(inactive='=10'), added_at=NOW - 10 * D), "never seen, bot added 10d ago: =10")

# joined: whole UTC days
day = int(datetime(2025, 1, 15, tzinfo=timezone.utc).timestamp())
morning, evening = m(join_date=day + 3600), m(join_date=day + 23 * 3600)
check(match(morning, f(joined='=2025-01-15')) and match(evening, f(joined='=2025-01-15')), "joined:=2025-01-15 matches the whole day (never matched before)")
check(not match(m(join_date=day - 1), f(joined='=2025-01-15')) and not match(m(join_date=day + D), f(joined='=2025-01-15')), "=date excludes the day before/after")
check(not match(evening, f(joined='>2025-01-15')) and match(m(join_date=day + D), f(joined='>2025-01-15')), ">date starts the next day")
check(match(m(join_date=day - 1), f(joined='<2025-01-15')) and not match(morning, f(joined='<2025-01-15')), "<date is before that day")

# departed: same day semantics, legacy last_seen fallback kept
check(match(m(is_active=0, left_date=day + 5 * 3600), f(departed='=2025-01-15')), "departed:=date matches a leave that day")
check(match(m(is_active=0, left_date=None, last_seen=day + 5 * 3600), f(departed='=2025-01-15')), "departed falls back to last_seen for legacy rows")
check(not match(m(is_active=1, left_date=day + 5 * 3600), f(departed='=2025-01-15')), "rejoined member (active) not listed as departed")
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED"); sys.exit(1 if failures else 0)

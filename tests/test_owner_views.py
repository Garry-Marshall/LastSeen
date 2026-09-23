"""Verify admin/result views only respond to the user who ran the command."""
import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import discord
from bot.locale import load_locales, t
from cogs.commands import (OwnerOnlyView, PaginationView, SearchResultsView, JourneyView,
                           UserStatsView, LeaderboardView)

OWNER, STRANGER = 111, 222
failures = []


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        failures.append(msg)


class FakeResponse:
    def __init__(self): self.sent = []
    async def send_message(self, content=None, **kw): self.sent.append((content, kw))
    def is_done(self): return bool(self.sent)


def fake_interaction(user_id, custom_id="x"):
    return SimpleNamespace(user=SimpleNamespace(id=user_id), response=FakeResponse(),
                           data={"custom_id": custom_id, "component_type": 2, "values": []})


async def press(view, item, user_id):
    """Run discord.py's own dispatch path for a component press; report whether the callback ran."""
    ran = []
    async def cb(interaction): ran.append(True)
    item.callback = cb
    inter = fake_interaction(user_id)
    await view._scheduled_task(item, inter)
    return bool(ran), inter.response.sent


async def main():
    load_locales()
    embed = discord.Embed(title="p")
    cog = SimpleNamespace()
    views = {
        "PaginationView (/inactive export)": (PaginationView([embed, embed], OWNER, export_members=[{'user_id': 1}], lang='nl'), "export_csv_button"),
        "SearchResultsView (export)": (SearchResultsView([{'username': 'a'}], {}, OWNER, lang='en'), "export_csv_button"),
        "JourneyView": (JourneyView(cog, 1, 2, "u", OWNER, 'en'), "journey_button"),
        "UserStatsView (export report)": (UserStatsView(1, None, OWNER, 'en'), "export_button"),
        "LeaderboardView (back)": (LeaderboardView(1, None, OWNER, 'en'), "back_button"),
    }
    stats = views["UserStatsView (export report)"][0]
    back = stats._make_back_view()
    views["UserStatsView back view"] = (back, None)

    for name, (view, attr) in views.items():
        item = getattr(view, attr) if attr else view.children[0]
        ran, sent = await press(view, item, STRANGER)
        check(not ran and len(sent) == 1 and sent[0][1].get('ephemeral'), f"{name}: stranger blocked with ephemeral notice")
        ran, sent = await press(view, item, OWNER)
        check(ran and not sent, f"{name}: owner allowed")

    # Localized notice uses the view's language
    view = views["PaginationView (/inactive export)"][0]
    _, sent = await press(view, view.next_button, STRANGER)
    check(sent[0][0] == t("errors.not_your_panel", 'nl'), "notice is in the guild language (nl)")

    # Owner propagates to child views
    check(back.owner_id == OWNER, "back view keeps owner")
    check(LeaderboardView(1, None, stats.owner_id, stats.lang).owner_id == OWNER, "leaderboard keeps owner")
    check(isinstance(stats, OwnerOnlyView) and stats.lang == 'en', "UserStatsView lang still set")

asyncio.run(main())
print("\nALL PASSED" if not failures else f"\n{len(failures)} FAILED")
sys.exit(1 if failures else 0)

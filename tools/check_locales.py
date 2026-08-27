#!/usr/bin/env python3
"""Locale consistency checker for LastSeen.

Run from the project root:  python tools/check_locales.py

Validates three things and exits non-zero if any fail:

  1. Every t("key") referenced in the code exists in the canonical English
     catalog (locales/en.json). A missing key means a runtime fallback to the
     raw key string. Checked on direct literal calls; keys reached dynamically
     (f-strings) or indirectly (a literal held in a dict/tuple/ternary and
     passed to t() by variable) are resolved for the unused-key check below but
     can't be validated to a concrete key here.

  2. Every other locale has exactly the same key set as English. Keys missing
     from a locale fall back to English at runtime (reported as warnings, not
     failures); keys present in a locale but not in English are orphans/typos
     (reported as failures).

  3. Placeholder parity: each translated value uses the same set of named
     {placeholders} as its English counterpart. A translator dropping or
     renaming a placeholder (e.g. {count} -> {aantal}) would crash str.format
     at runtime — this catches it before release.

  4. Discord length limits: modal titles and input labels are capped at 45
     characters by Discord, and the description line under a label at 100. A
     longer value (common when a translation runs longer than the English
     source) crashes with 50035 Invalid Form Body the moment the modal opens.
     Checked in every locale, English included.

  5. English keys that nothing in the code can reach (dead catalog entries),
     reported as warnings — harmless at runtime, just unused weight. Dynamic
     and indirect references (see 1) are resolved first so they aren't
     mistaken for dead keys.

Duplicate keys within a single JSON file are also reported.
"""

import json
import re
import string
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCALE_DIR = PROJECT_ROOT / "locales"
DEFAULT_LANG = "en"
CODE_DIRS = ["bot", "cogs"]

# Discord caps modal titles and input labels at 45 characters, and the optional
# description under a label at 100. Keys whose values are used in either place
# must respect that limit in every locale.
DISCORD_LABEL_MAX = 45
DISCORD_LABEL_DESC_MAX = 100
LABEL_KEY_SUFFIXES = ("_label", "modal_title")
LABEL_DESC_KEY_SUFFIXES = ("label_desc",)

_KEY_RE = re.compile(r'''\bt\(\s*["']([^"']+)["']''')
# A key can reach t() without appearing as a literal argument, in two ways this
# code base uses heavily:
#   - Dynamically, as an f-string: t(f"weekday.{name}"), t(f"...pulse_{state}").
#     We capture the literal prefix before the first {placeholder} and treat any
#     catalog key under that prefix as referenced.
#   - Indirectly, as a literal held in a dict / tuple / ternary that is then
#     passed to t() by variable (the leaderboard-period and profile-trend maps,
#     the weekly/monthly report title, meta.language_name via .get(), ...). We
#     treat any dotted string literal that exactly matches a catalog key as a
#     reference.
_FSTR_PREFIX_RE = re.compile(r'''\bt\(\s*f["']([^"'{}]*)\{''')
_DOTTED_LITERAL_RE = re.compile(r'''["']([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+)["']''')
_FORMATTER = string.Formatter()


def _placeholders(template: str) -> set:
    """Return the set of named placeholder fields in a format template."""
    return {name for _, name, _, _ in _FORMATTER.parse(template) if name}


def _load_with_dup_check(path: Path, problems: list) -> dict:
    """Load a JSON locale file, recording any duplicate keys as problems."""
    seen = set()
    dups = []

    def hook(pairs):
        for k, _ in pairs:
            if k in seen:
                dups.append(k)
            seen.add(k)
        return dict(pairs)

    with open(path, encoding="utf-8") as f:
        data = json.load(f, object_pairs_hook=hook)
    for k in dups:
        problems.append(f"{path.name}: duplicate key {k!r}")
    return data


def _scan_code() -> tuple:
    """Scan the code base once for the three reference forms.

    Returns (direct, prefixes, literals):
      direct    keys used as a literal t("key") argument
      prefixes  literal prefixes of dynamic f-string keys, t(f"prefix{...")
      literals  every dotted string literal in the source (a superset of keys
                held in dicts/tuples/ternaries and later passed to t())
    """
    direct, prefixes, literals = set(), set(), set()
    for d in CODE_DIRS:
        for path in (PROJECT_ROOT / d).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            direct |= set(_KEY_RE.findall(text))
            prefixes |= {p for p in _FSTR_PREFIX_RE.findall(text) if p}
            literals |= set(_DOTTED_LITERAL_RE.findall(text))
    return direct, prefixes, literals


def _resolve_referenced(en_keys: set, direct: set, prefixes: set, literals: set) -> set:
    """Catalog keys reachable at runtime, across all three reference forms.

    Direct literal calls, plus keys held in variables/dicts and passed to t()
    (matched as exact dotted literals), plus keys built from an f-string prefix.
    The prefix match is deliberately permissive: it can mask a genuinely dead
    key that happens to share a live prefix, which is preferable to falsely
    flagging a dynamically-built key as unused.
    """
    referenced = set(direct)
    referenced |= en_keys & literals
    if prefixes:
        referenced |= {k for k in en_keys if any(k.startswith(p) for p in prefixes)}
    return referenced


def main() -> int:
    problems = []   # hard failures -> exit 1
    warnings = []   # soft issues (runtime falls back to English)

    locale_files = sorted(LOCALE_DIR.glob("*.json"))
    if not locale_files:
        print(f"No locale files found in {LOCALE_DIR}")
        return 1

    catalogs = {p.stem: _load_with_dup_check(p, problems) for p in locale_files}

    if DEFAULT_LANG not in catalogs:
        print(f"Default locale '{DEFAULT_LANG}.json' is missing")
        return 1

    en = catalogs[DEFAULT_LANG]
    en_keys = set(en)

    # 1. code references must exist in English. Validated on direct literal
    #    t("key") calls only — dynamic (f-string) and indirect (variable) refs
    #    can't be resolved to a concrete key to check here.
    direct, prefixes, literals = _scan_code()
    referenced = _resolve_referenced(en_keys, direct, prefixes, literals)
    for key in sorted(direct - en_keys):
        problems.append(f"code references t({key!r}) but it is missing from {DEFAULT_LANG}.json")

    # 2 & 3. each other locale vs English
    for lang, cat in catalogs.items():
        if lang == DEFAULT_LANG:
            continue
        cat_keys = set(cat)

        for key in sorted(en_keys - cat_keys):
            warnings.append(f"{lang}.json: missing key {key!r} (will fall back to English)")
        for key in sorted(cat_keys - en_keys):
            problems.append(f"{lang}.json: orphan key {key!r} not present in {DEFAULT_LANG}.json")

        for key in sorted(en_keys & cat_keys):
            want = _placeholders(en[key])
            got = _placeholders(cat[key])
            if want != got:
                missing = want - got
                extra = got - want
                detail = []
                if missing:
                    detail.append(f"missing {sorted(missing)}")
                if extra:
                    detail.append(f"unexpected {sorted(extra)}")
                problems.append(f"{lang}.json: placeholder mismatch in {key!r} ({'; '.join(detail)})")

    # 4. Discord length limits on modal titles and TextInput labels (all locales)
    for lang, cat in catalogs.items():
        for key, value in cat.items():
            if not isinstance(value, str):
                continue
            if key.endswith(LABEL_DESC_KEY_SUFFIXES):
                if len(value) > DISCORD_LABEL_DESC_MAX:
                    problems.append(
                        f"{lang}.json: {key!r} is {len(value)} chars, exceeds Discord's "
                        f"{DISCORD_LABEL_DESC_MAX}-char limit for label descriptions"
                    )
            elif key.endswith(LABEL_KEY_SUFFIXES) and len(value) > DISCORD_LABEL_MAX:
                problems.append(
                    f"{lang}.json: {key!r} is {len(value)} chars, exceeds Discord's "
                    f"{DISCORD_LABEL_MAX}-char limit for modal titles/labels"
                )

    # 5. English keys nothing in the code can reach — dead catalog entries.
    #    Harmless at runtime (just unused weight), so reported as warnings. Only
    #    meaningful now that dynamic/indirect references are resolved above;
    #    the permissive prefix match means this can miss a dead key sharing a
    #    live prefix, never the reverse.
    for key in sorted(en_keys - referenced):
        warnings.append(f"{DEFAULT_LANG}.json: key {key!r} appears unused (no reference found in code)")

    langs = ", ".join(sorted(catalogs))
    print(f"Checked {len(catalogs)} locale(s): {langs}")
    print(f"  {len(en_keys)} keys in {DEFAULT_LANG}.json, {len(referenced)} referenced in code")

    for w in warnings:
        print(f"  WARNING: {w}")
    for p in problems:
        print(f"  ERROR:   {p}")

    if problems:
        print(f"\nFAILED with {len(problems)} error(s)" + (f", {len(warnings)} warning(s)" if warnings else ""))
        return 1
    print("\nOK" + (f" ({len(warnings)} warning(s))" if warnings else " — all locales consistent"))
    return 0


if __name__ == "__main__":
    sys.exit(main())

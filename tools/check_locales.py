#!/usr/bin/env python3
"""Locale consistency checker for LastSeen.

Run from the project root:  python tools/check_locales.py

Validates three things and exits non-zero if any fail:

  1. Every t("key") referenced in the code exists in the canonical English
     catalog (locales/en.json). A missing key means a runtime fallback to the
     raw key string.

  2. Every other locale has exactly the same key set as English. Keys missing
     from a locale fall back to English at runtime (reported as warnings, not
     failures); keys present in a locale but not in English are orphans/typos
     (reported as failures).

  3. Placeholder parity: each translated value uses the same set of named
     {placeholders} as its English counterpart. A translator dropping or
     renaming a placeholder (e.g. {count} -> {aantal}) would crash str.format
     at runtime — this catches it before release.

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

_KEY_RE = re.compile(r'''\bt\(\s*["']([^"']+)["']''')
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


def _referenced_keys() -> set:
    """All t("...") keys referenced across the code base."""
    keys = set()
    for d in CODE_DIRS:
        for path in (PROJECT_ROOT / d).rglob("*.py"):
            keys |= set(_KEY_RE.findall(path.read_text(encoding="utf-8")))
    return keys


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

    # 1. code references must exist in English
    used = _referenced_keys()
    for key in sorted(used - en_keys):
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

    langs = ", ".join(sorted(catalogs))
    print(f"Checked {len(catalogs)} locale(s): {langs}")
    print(f"  {len(en_keys)} keys in {DEFAULT_LANG}.json, {len(used)} referenced in code")

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

"""Locale loading and translation for LastSeen bot.

Runtime user-facing strings live in ``locales/<lang>.json`` as flat
``key -> template`` maps. English (``en``) is the canonical catalog and the
fallback for any key missing from another language. Templates use named
placeholders filled via :meth:`str.format`, e.g. ``"Removed {count} record(s)"``.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_LANGUAGE = 'en'
_LOCALE_DIR = Path('locales')

# lang -> {key: template}
_catalogs: dict[str, dict[str, str]] = {}


def load_locales() -> None:
    """Load every ``locales/*.json`` file into memory. Call once at startup."""
    _catalogs.clear()

    if not _LOCALE_DIR.exists():
        logger.error(f"Locale directory '{_LOCALE_DIR}' not found; no translations loaded")
        return

    for path in sorted(_LOCALE_DIR.glob('*.json')):
        lang = path.stem
        try:
            with open(path, encoding='utf-8') as f:
                _catalogs[lang] = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Failed to load locale '{lang}' from {path}: {e}")

    if DEFAULT_LANGUAGE not in _catalogs:
        logger.error(
            f"Default locale '{DEFAULT_LANGUAGE}' missing; user-facing text will fall back to raw keys"
        )

    logger.info(f"Loaded {len(_catalogs)} locale(s): {', '.join(sorted(_catalogs)) or 'none'}")


def available_languages() -> list[str]:
    """Return the sorted list of loaded language codes."""
    return sorted(_catalogs)


def language_name(code: str) -> str:
    """Human-readable display name for a language code.

    Each locale self-describes its name via the ``meta.language_name`` key;
    falls back to the bare code if that key is missing.
    """
    return _catalogs.get(code, {}).get('meta.language_name', code)


# Canonical Monday-first weekday order, used to map an English name or a
# weekday index onto the ``weekday.<name>`` catalog keys.
_WEEKDAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')


def weekday_name(day, lang: str = DEFAULT_LANGUAGE) -> str:
    """Localized weekday name for display.

    ``day`` may be an English weekday name (e.g. ``"Monday"``) or a Monday=0
    index (0-6). Returns the value unchanged if it can't be resolved, so data
    from outside the known set is never lost.
    """
    if isinstance(day, int):
        if not 0 <= day < 7:
            return str(day)
        canonical = _WEEKDAYS[day]
    else:
        canonical = str(day).strip().lower()
        if canonical not in _WEEKDAYS:
            return str(day)
    return t(f'weekday.{canonical}', lang)


def guild_language(guild_config) -> str:
    """Resolve the language code for a guild from its config dict.

    Falls back to the default language when the guild has no config, no language
    set, or a language that isn't loaded.
    """
    if not guild_config:
        return DEFAULT_LANGUAGE
    lang = guild_config.get('language') or DEFAULT_LANGUAGE
    return lang if lang in _catalogs else DEFAULT_LANGUAGE


def t(key: str, lang: str = DEFAULT_LANGUAGE, **kwargs) -> str:
    """Translate ``key`` into ``lang``.

    Falls back to the default language if the key is missing for ``lang``, then
    to the raw key if it is missing everywhere. Remaining ``kwargs`` fill named
    placeholders via :meth:`str.format`.
    """
    template = _catalogs.get(lang, {}).get(key)
    if template is None:
        template = _catalogs.get(DEFAULT_LANGUAGE, {}).get(key)
    if template is None:
        logger.warning(f"Missing translation key: {key!r}")
        return key

    try:
        return template.format(**kwargs)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Failed to format translation key {key!r} ({lang}): {e}")
        return template

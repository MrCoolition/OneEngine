from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


_SPACE_RE = re.compile(r"\s+")
_FIELD_RE = re.compile(r"[^a-z0-9]+")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
    if isinstance(value, Decimal):
        return format(value, "f").rstrip("0").rstrip(".")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return _SPACE_RE.sub(" ", str(value).strip())


def canonical_field(value: Any) -> str:
    return _FIELD_RE.sub("_", clean_text(value).lower()).strip("_")


def normalized_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", clean_text(value).upper()).strip()


def canonical_action(value: Any) -> str:
    text = clean_text(value)
    key = normalized_key(text)
    aliases = {
        "1 X": "1X",
        "1X": "1X",
        "APPROVED 1 X": "Approved - 1X",
        "APPROVED 1X": "Approved - 1X",
        "FIND ALT 1ST": "Find Alt First",
        "FIND ALT FIRST": "Find Alt First",
        "OK": "OK",
        "BLANK": "",
    }
    return aliases.get(key, text)


def parse_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        return None if math.isnan(number) else number
    text = clean_text(value).replace(",", "").strip()
    if not text or text.lower() == "blank":
        return None
    percent = text.endswith("%")
    if percent:
        text = text[:-1].strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    return number / 100.0 if percent else number


def stable_value(value: Any) -> str:
    number = parse_number(value)
    if number is not None and isinstance(value, (int, float, Decimal)):
        return f"n:{number:.12g}"
    return f"s:{normalized_key(value)}"

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from ..models import DistillationProfile
from ..normalization import canonical_field, clean_text, normalized_key, parse_number


def _lower(value: Any) -> str:
    return clean_text(value).lower()


def _is_one_time(value: Any) -> bool:
    return bool(re.search(r"one-time|one time|seasonal", clean_text(value), re.I))


def project_features(
    row: Mapping[str, Any],
    profile: DistillationProfile,
) -> Mapping[str, Any]:
    del profile
    values = {canonical_field(key): value for key, value in row.items()}
    values["input_action"] = values.get("action")
    values["input_if_in_stock_action"] = values.get("if_in_stock_action")
    values["input_buysmart_action"] = values.get("buysmart_action")

    business = clean_text(values.get("business"))
    request_type = clean_text(values.get("type"))
    sector = _lower(values.get("sector"))
    division = _lower(values.get("division"))
    compass_apl = _lower(values.get("compass_apl"))
    pantry = _lower(values.get("pantry"))
    in_cat = _lower(values.get("in_cat"))
    duration = clean_text(values.get("one_time_or_permanent"))
    conversion_din = clean_text(values.get("conversion_din"))

    values.update(
        {
            "business_key": normalized_key(business),
            "request_type_key": normalized_key(request_type),
            "usage_num": parse_number(values.get("usage")),
            "meets_criteria_num": parse_number(values.get("meets_criteria")),
            "conversion_va_num": parse_number(values.get("conversion_va")),
            "is_one_time": _is_one_time(duration),
            "is_permanent": "permanent" in duration.lower(),
            "is_in_cat_y": in_cat == "y",
            "is_temp_available": "temp available" in in_cat or in_cat == "ta",
            "is_in_catalog": in_cat == "y" or "temp available" in in_cat,
            "is_pantry": "item" in pantry or "subcategory" in pantry or pantry == "y",
            "is_k12_apl": _lower(values.get("k12_apl")) == "y",
            "is_core_apl": "core apl" in compass_apl,
            "is_s1": bool(re.search(r"\bs1\b", compass_apl, re.I)),
            "is_foh": "front of house" in compass_apl
            or bool(re.search(r"\bfoh\b", compass_apl, re.I)),
            "is_diverse": "diverse" in compass_apl,
            "has_conversion": bool(conversion_din),
            "is_levy": "levy" in sector or "levy" in division,
            "is_schools": "school" in division or "chartwells" in division,
        }
    )
    return values

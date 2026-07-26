from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import (
    DistillationProfile,
    InductionConfig,
    MatchingConfig,
    OutputField,
)


DEFAULT_PROFILE_DIR = Path(__file__).resolve().parent / "profile_catalog"


def _tuple_groups(values: list[list[str]]) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(str(item) for item in group) for group in values)


def profile_from_dict(payload: dict[str, Any]) -> DistillationProfile:
    outputs = tuple(
        OutputField(
            source=str(item["source"]),
            target=str(item["target"]),
            action_type=str(item["action_type"]),
            normalizer=str(item.get("normalizer", "action")),
        )
        for item in payload["output_fields"]
    )
    matching_payload = payload["matching"]
    matching = MatchingConfig(
        identity_groups=_tuple_groups(matching_payload.get("identity_groups", [])),
        ignored_fields=frozenset(matching_payload.get("ignored_fields", [])),
        volatile_fields=frozenset(matching_payload.get("volatile_fields", [])),
        similarity_fields=tuple(matching_payload.get("similarity_fields", [])),
        minimum_similarity=float(matching_payload.get("minimum_similarity", 0.72)),
        ambiguity_margin=float(matching_payload.get("ambiguity_margin", 0.05)),
    )
    induction_payload = payload["induction"]
    induction = InductionConfig(
        feature_fields=tuple(induction_payload.get("feature_fields", [])),
        numeric_fields=frozenset(induction_payload.get("numeric_fields", [])),
        token_fields=frozenset(induction_payload.get("token_fields", [])),
        excluded_fields=frozenset(induction_payload.get("excluded_fields", [])),
        minimum_leaf_size=int(induction_payload.get("minimum_leaf_size", 2)),
        maximum_depth=int(induction_payload.get("maximum_depth", 16)),
        minimum_gain=float(induction_payload.get("minimum_gain", 1e-9)),
        maximum_category_splits=int(
            induction_payload.get("maximum_category_splits", 64)
        ),
        maximum_numeric_splits=int(
            induction_payload.get("maximum_numeric_splits", 64)
        ),
        maximum_token_splits=int(
            induction_payload.get("maximum_token_splits", 96)
        ),
        minimum_token_support=int(induction_payload.get("minimum_token_support", 3)),
        minimum_general_support=int(
            induction_payload.get("minimum_general_support", 3)
        ),
        maximum_general_rules_per_label=int(
            induction_payload.get("maximum_general_rules_per_label", 16)
        ),
        exception_identity_groups=_tuple_groups(
            induction_payload.get(
                "exception_identity_groups",
                matching_payload.get("identity_groups", []),
            )
        ),
        validation_group=str(induction_payload.get("validation_group", "source_group")),
    )
    return DistillationProfile(
        profile_id=str(payload["profile_id"]),
        version=str(payload["version"]),
        description=str(payload.get("description", "")),
        output_fields=outputs,
        matching=matching,
        induction=induction,
        column_aliases={
            str(key): str(value)
            for key, value in payload.get("column_aliases", {}).items()
        },
        feature_projector=payload.get("feature_projector"),
        metadata=payload.get("metadata", {}),
    )


def load_profile(value: str | Path) -> DistillationProfile:
    candidate = Path(value)
    if not candidate.exists():
        candidate = DEFAULT_PROFILE_DIR / f"{value}.json"
    if not candidate.exists():
        raise FileNotFoundError(f"Distillation profile not found: {value}")
    return profile_from_dict(json.loads(candidate.read_text(encoding="utf-8")))

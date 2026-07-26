from __future__ import annotations

import importlib
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .models import DistillationProfile, ProjectedPair, RowPair
from .normalization import canonical_field, clean_text, stable_value


FeatureProjector = Callable[
    [Mapping[str, Any], DistillationProfile],
    Mapping[str, Any],
]


def default_projector(
    row: Mapping[str, Any],
    profile: DistillationProfile,
) -> Mapping[str, Any]:
    return {canonical_field(key): value for key, value in row.items()}


def load_projector(profile: DistillationProfile) -> FeatureProjector:
    if not profile.feature_projector:
        return default_projector
    module_name, separator, attribute = profile.feature_projector.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError(
            f"Invalid feature projector reference: {profile.feature_projector!r}"
        )
    module = importlib.import_module(module_name)
    projector = getattr(module, attribute, None)
    if not callable(projector):
        raise TypeError(
            f"Feature projector is not callable: {profile.feature_projector!r}"
        )
    return projector


def evidence_hash(row: Mapping[str, Any]) -> str:
    """Fingerprint the immutable, canonical BEFORE payload used as evidence."""
    canonical: dict[str, Any] = {}
    for key, value in row.items():
        field = canonical_field(key)
        if not field:
            continue
        if field not in canonical or not clean_text(canonical[field]):
            canonical[field] = value
    payload = [
        (key, stable_value(value))
        for key, value in sorted(canonical.items())
    ]
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def project_pairs(
    pairs: Sequence[RowPair],
    profile: DistillationProfile,
) -> tuple[ProjectedPair, ...]:
    projector = load_projector(profile)
    projected: list[ProjectedPair] = []
    for pair in pairs:
        values = dict(projector(pair.before, profile))
        values["__evidence_hash"] = evidence_hash(pair.before)
        label = tuple(sorted(pair.outputs.items()))
        projected.append(ProjectedPair(pair=pair, features=values, label=label))
    return tuple(projected)

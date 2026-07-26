from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class OutputField:
    source: str
    target: str
    action_type: str
    normalizer: str = "action"


@dataclass(frozen=True)
class MatchingConfig:
    identity_groups: tuple[tuple[str, ...], ...]
    ignored_fields: frozenset[str] = frozenset()
    volatile_fields: frozenset[str] = frozenset()
    similarity_fields: tuple[str, ...] = ()
    minimum_similarity: float = 0.72
    ambiguity_margin: float = 0.05


@dataclass(frozen=True)
class InductionConfig:
    feature_fields: tuple[str, ...]
    numeric_fields: frozenset[str] = frozenset()
    token_fields: frozenset[str] = frozenset()
    excluded_fields: frozenset[str] = frozenset()
    minimum_leaf_size: int = 2
    maximum_depth: int = 16
    minimum_gain: float = 1e-9
    maximum_category_splits: int = 64
    maximum_numeric_splits: int = 64
    maximum_token_splits: int = 96
    minimum_token_support: int = 3
    minimum_general_support: int = 3
    maximum_general_rules_per_label: int = 16
    exception_identity_groups: tuple[tuple[str, ...], ...] = ()
    validation_group: str = "source_group"


@dataclass(frozen=True)
class DistillationProfile:
    profile_id: str
    version: str
    description: str
    output_fields: tuple[OutputField, ...]
    matching: MatchingConfig
    induction: InductionConfig
    column_aliases: Mapping[str, str] = field(default_factory=dict)
    feature_projector: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def output_source_fields(self) -> frozenset[str]:
        return frozenset(item.source for item in self.output_fields)


@dataclass(frozen=True)
class Document:
    name: str
    source_type: str
    rows: tuple[Mapping[str, Any], ...]
    source_group: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MatchEvidence:
    method: str
    score: float
    identity_fields: tuple[str, ...] = ()
    changed_input_fields: tuple[str, ...] = ()
    ambiguous: bool = False
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RowPair:
    pair_id: str
    source_group: str
    before_index: int
    after_index: int
    before: Mapping[str, Any]
    after: Mapping[str, Any]
    outputs: Mapping[str, Any]
    evidence: MatchEvidence


@dataclass(frozen=True)
class UnmatchedRow:
    side: str
    source_group: str
    row_index: int
    row: Mapping[str, Any]
    reason: str


@dataclass(frozen=True)
class MatchResult:
    pairs: tuple[RowPair, ...]
    unmatched: tuple[UnmatchedRow, ...]
    method_counts: Mapping[str, int]


@dataclass(frozen=True)
class PredicateAtom:
    field: str
    operator: str
    value: Any = None


@dataclass(frozen=True)
class ProjectedPair:
    pair: RowPair
    features: Mapping[str, Any]
    label: tuple[tuple[str, Any], ...]


@dataclass(frozen=True)
class DistilledRule:
    rule_id: str
    priority: int
    predicates: tuple[PredicateAtom, ...]
    outputs: Mapping[str, Any]
    support: int
    confidence: float
    source_groups: tuple[str, ...]
    validation_accuracy: float
    kind: str = "general"
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationResult:
    row_count: int
    matched_count: int
    exact_count: int
    accuracy: float
    contradictions: int
    uncovered_pair_ids: tuple[str, ...] = ()
    mismatched_pair_ids: tuple[str, ...] = ()
    by_source_group: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class DistillationRun:
    run_id: str
    profile_id: str
    profile_version: str
    created_at: str
    source_paths: tuple[Path, ...]
    match_result: MatchResult
    rules: tuple[DistilledRule, ...]
    validation: ValidationResult
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

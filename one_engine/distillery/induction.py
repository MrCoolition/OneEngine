from __future__ import annotations

import hashlib
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .models import (
    DistillationProfile,
    DistilledRule,
    PredicateAtom,
    ProjectedPair,
)
from .normalization import canonical_field, clean_text, normalized_key, parse_number


Label = tuple[tuple[str, Any], ...]

_STOP_WORDS = {
    "and",
    "are",
    "for",
    "from",
    "into",
    "not",
    "the",
    "this",
    "with",
    "without",
    "item",
    "product",
    "case",
    "each",
    "pack",
    "count",
}
_IN_OPTION_CACHE: dict[int, tuple[Any, frozenset[str]]] = {}


@dataclass(frozen=True)
class _Split:
    atom: PredicateAtom
    gain: float
    true_indices: tuple[int, ...]
    false_indices: tuple[int, ...]


@dataclass(frozen=True)
class _Leaf:
    path: tuple[PredicateAtom, ...]
    indices: tuple[int, ...]
    predicted: Label
    confidence: float


def _entropy(labels: Iterable[Label]) -> float:
    counts = Counter(labels)
    total = sum(counts.values())
    if not total:
        return 0.0
    return -sum(
        (count / total) * math.log2(count / total)
        for count in counts.values()
        if count
    )


def evaluate_atom(atom: PredicateAtom, features: Mapping[str, Any]) -> bool:
    left = features.get(atom.field)
    right = atom.value
    operator = atom.operator
    if operator == "eq":
        return normalized_key(left) == normalized_key(right)
    if operator == "ne":
        return normalized_key(left) != normalized_key(right)
    if operator == "in":
        is_collection = isinstance(right, (list, tuple, set))
        options = right if is_collection else [right]
        cache_key = id(right) if is_collection else 0
        cached = _IN_OPTION_CACHE.get(cache_key)
        normalized_options = cached[1] if cached and cached[0] is right else None
        if normalized_options is None:
            normalized_options = frozenset(normalized_key(item) for item in options)
            if is_collection:
                _IN_OPTION_CACHE[cache_key] = (right, normalized_options)
        return normalized_key(left) in normalized_options
    if operator == "not_in":
        is_collection = isinstance(right, (list, tuple, set))
        options = right if is_collection else [right]
        cache_key = id(right) if is_collection else 0
        cached = _IN_OPTION_CACHE.get(cache_key)
        normalized_options = cached[1] if cached and cached[0] is right else None
        if normalized_options is None:
            normalized_options = frozenset(normalized_key(item) for item in options)
            if is_collection:
                _IN_OPTION_CACHE[cache_key] = (right, normalized_options)
        return normalized_key(left) not in normalized_options
    if operator == "blank":
        return not clean_text(left)
    if operator == "not_blank":
        return bool(clean_text(left))
    if operator == "is_true":
        return bool(left)
    if operator == "is_false":
        return not bool(left)
    if operator == "contains":
        return clean_text(right).lower() in clean_text(left).lower()
    if operator == "not_contains":
        return clean_text(right).lower() not in clean_text(left).lower()
    left_number = parse_number(left)
    right_number = parse_number(right)
    if left_number is None or right_number is None:
        return False
    if operator == "ge":
        return left_number >= right_number
    if operator == "gt":
        return left_number > right_number
    if operator == "lt":
        return left_number < right_number
    if operator == "le":
        return left_number <= right_number
    return False


def inverse_atom(atom: PredicateAtom) -> PredicateAtom:
    operators = {
        "eq": "ne",
        "ne": "eq",
        "in": "not_in",
        "not_in": "in",
        "blank": "not_blank",
        "not_blank": "blank",
        "is_true": "is_false",
        "is_false": "is_true",
        "contains": "not_contains",
        "not_contains": "contains",
        "ge": "lt",
        "lt": "ge",
        "gt": "le",
        "le": "gt",
    }
    return PredicateAtom(atom.field, operators[atom.operator], atom.value)


def _tokenize(value: Any) -> set[str]:
    tokens = {
        token.lower()
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9&'/-]{2,}", clean_text(value))
    }
    return {token for token in tokens if token not in _STOP_WORDS}


def _sample(values: Sequence[Any], maximum: int) -> list[Any]:
    if len(values) <= maximum:
        return list(values)
    if maximum <= 1:
        return [values[len(values) // 2]]
    indexes = {
        round(index * (len(values) - 1) / (maximum - 1))
        for index in range(maximum)
    }
    return [values[index] for index in sorted(indexes)]


class RuleInducer:
    def __init__(self, profile: DistillationProfile):
        self.profile = profile
        self.rows: Sequence[ProjectedPair] = ()
        self._candidate_coverages: tuple[
            tuple[PredicateAtom, frozenset[int]], ...
        ] = ()
        self._label_coverages: Mapping[Label, frozenset[int]] = {}
        self._identity_labels: Mapping[
            tuple[str, ...],
            Mapping[tuple[str, ...], frozenset[Label]],
        ] = {}
        self._normalized_values: Mapping[str, tuple[str, ...]] = {}
        self._lower_values: Mapping[str, tuple[str, ...]] = {}
        self._numeric_values: Mapping[str, tuple[float | None, ...]] = {}
        self._token_values: Mapping[str, tuple[frozenset[str], ...]] = {}
        self._candidate_matrix: Any = None
        self._label_codes: Any = None
        self._label_count = 0

    def _prepare_feature_caches(self) -> None:
        fields = {
            field
            for field in self.profile.induction.feature_fields
            if field != "*"
        }
        fields.update(
            canonical_field(field)
            for group in self.profile.induction.exception_identity_groups
            for field in group
        )
        normalized: dict[str, tuple[str, ...]] = {}
        lowered: dict[str, tuple[str, ...]] = {}
        numeric: dict[str, tuple[float | None, ...]] = {}
        tokens: dict[str, tuple[frozenset[str], ...]] = {}
        for field in fields:
            values = tuple(row.features.get(field) for row in self.rows)
            normalized[field] = tuple(normalized_key(value) for value in values)
            lowered[field] = tuple(clean_text(value).lower() for value in values)
            if field in self.profile.induction.numeric_fields:
                numeric[field] = tuple(parse_number(value) for value in values)
            if field in self.profile.induction.token_fields:
                tokens[field] = tuple(
                    frozenset(_tokenize(value)) for value in values
                )
        self._normalized_values = normalized
        self._lower_values = lowered
        self._numeric_values = numeric
        self._token_values = tokens

    def _candidate_atoms(self, indices: Sequence[int]) -> Iterable[PredicateAtom]:
        config = self.profile.induction
        fields = config.feature_fields
        for field in fields:
            if field == "*":
                continue
            values = [self.rows[index].features.get(field) for index in indices]
            normalized_values = self._normalized_values[field]
            if field in config.numeric_fields:
                numeric_values = sorted(
                    {
                        number
                        for index in indices
                        if (number := self._numeric_values[field][index]) is not None
                    }
                )
                if len(numeric_values) > 1:
                    thresholds = [
                        (left + right) / 2.0
                        for left, right in zip(numeric_values, numeric_values[1:])
                    ]
                    for threshold in _sample(
                        thresholds,
                        config.maximum_numeric_splits,
                    ):
                        yield PredicateAtom(field, "ge", threshold)
                if any(value is None or not clean_text(value) for value in values):
                    yield PredicateAtom(field, "blank")
                continue

            non_blank_indices = [
                index for index in indices if normalized_values[index]
            ]
            non_blank = [self.rows[index].features.get(field) for index in non_blank_indices]
            if len(non_blank_indices) < len(indices) and non_blank:
                yield PredicateAtom(field, "blank")
            if not non_blank:
                continue
            if all(isinstance(value, bool) for value in non_blank):
                yield PredicateAtom(field, "is_true")
                continue

            value_counts = Counter(normalized_values[index] for index in non_blank_indices)
            display_values: dict[str, Any] = {}
            for index in non_blank_indices:
                display_values.setdefault(
                    normalized_values[index],
                    self.rows[index].features.get(field),
                )
            selected_values = [
                value
                for value, _ in value_counts.most_common(
                    config.maximum_category_splits
                )
            ]
            for value in selected_values:
                yield PredicateAtom(field, "eq", display_values[value])

            labels_by_value: dict[str, Counter[Label]] = defaultdict(Counter)
            for index in indices:
                value = self.rows[index].features.get(field)
                if normalized_values[index]:
                    labels_by_value[normalized_values[index]][self.rows[index].label] += 1
            values_by_majority: dict[Label, list[str]] = defaultdict(list)
            for value, label_counts in labels_by_value.items():
                majority, _ = label_counts.most_common(1)[0]
                values_by_majority[majority].append(value)
            for grouped_values in values_by_majority.values():
                if 1 < len(grouped_values) <= config.maximum_category_splits:
                    ordered = sorted(
                        grouped_values,
                        key=lambda value: value_counts[value],
                        reverse=True,
                    )
                    yield PredicateAtom(
                        field,
                        "in",
                        [display_values[value] for value in ordered],
                    )

            if field in config.token_fields:
                token_counts: Counter[str] = Counter()
                for index in indices:
                    token_counts.update(self._token_values[field][index])
                tokens = [
                    token
                    for token, count in token_counts.most_common(
                        config.maximum_token_splits
                    )
                    if count >= config.minimum_token_support
                    and count < len(indices)
                ]
                for token in tokens:
                    yield PredicateAtom(field, "contains", token)

    def _coverage_for_atom(self, atom: PredicateAtom) -> frozenset[int]:
        field = atom.field
        operator = atom.operator
        if operator in {"eq", "ne", "in", "not_in", "blank", "not_blank"}:
            values = self._normalized_values[field]
            if operator in {"eq", "ne"}:
                target = normalized_key(atom.value)
                matched = {
                    index for index, value in enumerate(values) if value == target
                }
                return frozenset(
                    matched
                    if operator == "eq"
                    else set(range(len(values))) - matched
                )
            if operator in {"in", "not_in"}:
                options = atom.value if isinstance(atom.value, (list, tuple, set)) else [atom.value]
                targets = {normalized_key(value) for value in options}
                matched = {
                    index for index, value in enumerate(values) if value in targets
                }
                return frozenset(
                    matched
                    if operator == "in"
                    else set(range(len(values))) - matched
                )
            matched = {index for index, value in enumerate(values) if not value}
            return frozenset(
                matched
                if operator == "blank"
                else set(range(len(values))) - matched
            )
        if operator in {"is_true", "is_false"}:
            matched = {
                index
                for index, row in enumerate(self.rows)
                if bool(row.features.get(field))
            }
            return frozenset(
                matched
                if operator == "is_true"
                else set(range(len(self.rows))) - matched
            )
        if operator in {"contains", "not_contains"}:
            target = clean_text(atom.value).lower()
            matched = {
                index
                for index, value in enumerate(self._lower_values[field])
                if target in value
            }
            return frozenset(
                matched
                if operator == "contains"
                else set(range(len(self.rows))) - matched
            )
        target = parse_number(atom.value)
        values = self._numeric_values[field]
        if target is None:
            return frozenset()
        comparators = {
            "ge": lambda value: value >= target,
            "gt": lambda value: value > target,
            "lt": lambda value: value < target,
            "le": lambda value: value <= target,
        }
        comparator = comparators[operator]
        return frozenset(
            index
            for index, value in enumerate(values)
            if value is not None and comparator(value)
        )

    def _prepare_indexes(self) -> None:
        self._prepare_feature_caches()
        all_indices = tuple(range(len(self.rows)))
        candidates: list[tuple[PredicateAtom, frozenset[int]]] = []
        seen: set[tuple[str, str, str]] = set()
        for atom in self._candidate_atoms(all_indices):
            signature = (atom.field, atom.operator, repr(atom.value))
            if signature in seen:
                continue
            seen.add(signature)
            coverage = self._coverage_for_atom(atom)
            if coverage and len(coverage) < len(self.rows):
                candidates.append((atom, coverage))
        label_indices: dict[Label, set[int]] = defaultdict(set)
        for index, row in enumerate(self.rows):
            label_indices[row.label].add(index)
        self._candidate_coverages = tuple(candidates)
        self._label_coverages = {
            label: frozenset(indices) for label, indices in label_indices.items()
        }
        try:
            import numpy as np
        except Exception as exc:  # pragma: no cover - environment failure
            raise RuntimeError("Vectorized induction requires numpy") from exc
        matrix = np.zeros(
            (len(self.rows), len(self._candidate_coverages)),
            dtype=np.bool_,
        )
        for column, (_, coverage) in enumerate(self._candidate_coverages):
            if coverage:
                matrix[np.fromiter(coverage, dtype=np.int64), column] = True
        label_values = list(self._label_coverages)
        label_code = {label: index for index, label in enumerate(label_values)}
        self._candidate_matrix = matrix
        self._label_codes = np.fromiter(
            (label_code[row.label] for row in self.rows),
            dtype=np.int32,
        )
        self._label_count = len(label_values)
        identity_labels: dict[
            tuple[str, ...],
            dict[tuple[str, ...], set[Label]],
        ] = {}
        for identity_group in self.profile.induction.exception_identity_groups:
            canonical_group = tuple(canonical_field(field) for field in identity_group)
            index: dict[tuple[str, ...], set[Label]] = defaultdict(set)
            for row in self.rows:
                key = tuple(
                    normalized_key(row.features.get(field)) for field in canonical_group
                )
                if key and all(key):
                    index[key].add(row.label)
            identity_labels[canonical_group] = index
        self._identity_labels = {
            group: {key: frozenset(labels) for key, labels in index.items()}
            for group, index in identity_labels.items()
        }

    def _learn_pure_rules(self) -> list[DistilledRule]:
        config = self.profile.induction
        all_indices = frozenset(range(len(self.rows)))
        rules: list[DistilledRule] = []
        rule_index = 0
        label_order = sorted(
            self._label_coverages,
            key=lambda label: len(self._label_coverages[label]),
            reverse=True,
        )
        for label in label_order:
            label_indices = self._label_coverages[label]
            if len(label_indices) < config.minimum_general_support:
                continue
            uncovered = set(label_indices)
            learned_for_label = 0
            while (
                len(uncovered) >= config.minimum_general_support
                and learned_for_label < config.maximum_general_rules_per_label
            ):
                coverage = all_indices
                predicates: list[PredicateAtom] = []
                used_signatures: set[tuple[str, str, str]] = set()
                completed = False
                for _ in range(config.maximum_depth):
                    positives = len(coverage & label_indices)
                    negatives = len(coverage) - positives
                    if negatives == 0 and len(coverage & uncovered) >= config.minimum_leaf_size:
                        completed = True
                        break
                    current_precision = positives / len(coverage) if coverage else 0.0
                    best: tuple[
                        tuple[float, int, int, int],
                        PredicateAtom,
                        frozenset[int],
                    ] | None = None
                    for atom, atom_coverage in self._candidate_coverages:
                        signature = (atom.field, atom.operator, repr(atom.value))
                        if signature in used_signatures:
                            continue
                        candidate = coverage & atom_coverage
                        if not candidate or candidate == coverage:
                            continue
                        uncovered_positives = len(candidate & uncovered)
                        if uncovered_positives < config.minimum_leaf_size:
                            continue
                        candidate_positives = len(candidate & label_indices)
                        candidate_negatives = len(candidate) - candidate_positives
                        precision = candidate_positives / len(candidate)
                        if precision <= current_precision and candidate_negatives >= negatives:
                            continue
                        score = (
                            precision,
                            negatives - candidate_negatives,
                            uncovered_positives,
                            -len(candidate),
                        )
                        if best is None or score > best[0]:
                            best = (score, atom, candidate)
                    if best is None:
                        break
                    _, atom, coverage = best
                    predicates.append(atom)
                    used_signatures.add((atom.field, atom.operator, repr(atom.value)))
                positives_covered = coverage & label_indices
                if (
                    not completed
                    and len(coverage) == len(positives_covered)
                    and len(positives_covered & uncovered) >= config.minimum_leaf_size
                ):
                    completed = True
                if not completed:
                    break
                newly_covered = positives_covered & uncovered
                if len(newly_covered) < config.minimum_leaf_size:
                    break
                source_groups = tuple(
                    sorted(
                        {
                            self.rows[index].pair.source_group
                            for index in positives_covered
                        }
                    )
                )
                rules.append(
                    DistilledRule(
                        rule_id=self._rule_id(
                            f"DISTILLED-{self.profile.profile_id.upper()}-GENERAL",
                            predicates,
                            label,
                        ),
                        priority=100_000 + rule_index,
                        predicates=tuple(predicates),
                        outputs=dict(label),
                        support=len(positives_covered),
                        confidence=1.0,
                        source_groups=source_groups,
                        validation_accuracy=0.0,
                        kind="general",
                        evidence_ids=tuple(
                            self.rows[index].pair.pair_id
                            for index in sorted(positives_covered)
                        ),
                    )
                )
                rule_index += 1
                learned_for_label += 1
                uncovered -= newly_covered
        return rules

    def _entropy_indices(self, indices: frozenset[int]) -> float:
        total = len(indices)
        if not total:
            return 0.0
        entropy = 0.0
        for coverage in self._label_coverages.values():
            count = len(indices & coverage)
            if count:
                probability = count / total
                entropy -= probability * math.log2(probability)
        return entropy

    def _best_split(self, indices: Sequence[int]) -> _Split | None:
        config = self.profile.induction
        try:
            import numpy as np
        except Exception as exc:  # pragma: no cover - environment failure
            raise RuntimeError("Vectorized induction requires numpy") from exc
        node_indices = np.asarray(indices, dtype=np.int64)
        node_labels = self._label_codes[node_indices]
        parent_counts = np.bincount(
            node_labels,
            minlength=self._label_count,
        ).astype(np.float64)
        parent_probabilities = parent_counts[parent_counts > 0] / len(node_indices)
        parent_entropy = float(
            -(parent_probabilities * np.log2(parent_probabilities)).sum()
        )
        if parent_entropy <= 0.0 or self._candidate_matrix.shape[1] == 0:
            return None
        matrix = self._candidate_matrix[node_indices]
        true_totals = matrix.sum(axis=0, dtype=np.int64)
        false_totals = len(node_indices) - true_totals
        valid = (
            (true_totals >= config.minimum_leaf_size)
            & (false_totals >= config.minimum_leaf_size)
        )
        if not bool(valid.any()):
            return None

        true_counts = np.zeros(
            (self._label_count, matrix.shape[1]),
            dtype=np.float64,
        )
        for label_code in range(self._label_count):
            label_mask = node_labels == label_code
            if bool(label_mask.any()):
                true_counts[label_code] = matrix[label_mask].sum(
                    axis=0,
                    dtype=np.int64,
                )
        false_counts = parent_counts[:, None] - true_counts

        def entropy_columns(counts: Any, totals: Any) -> Any:
            with np.errstate(divide="ignore", invalid="ignore"):
                probabilities = np.divide(
                    counts,
                    totals[None, :],
                    out=np.zeros_like(counts),
                    where=totals[None, :] > 0,
                )
                terms = np.where(
                    probabilities > 0,
                    probabilities * np.log2(probabilities),
                    0.0,
                )
            return -terms.sum(axis=0)

        true_entropy = entropy_columns(true_counts, true_totals)
        false_entropy = entropy_columns(false_counts, false_totals)
        child_entropy = (
            true_totals / len(node_indices) * true_entropy
            + false_totals / len(node_indices) * false_entropy
        )
        gains = parent_entropy - child_entropy
        gains[~valid] = -np.inf
        balance = np.minimum(true_totals, false_totals)
        scores = gains + balance / max(len(node_indices), 1) * 1e-12
        best_column = int(np.argmax(scores))
        gain = float(gains[best_column])
        if not math.isfinite(gain) or gain < config.minimum_gain:
            return None
        true_mask = matrix[:, best_column]
        atom = self._candidate_coverages[best_column][0]
        return _Split(
            atom=atom,
            gain=gain,
            true_indices=tuple(int(value) for value in node_indices[true_mask]),
            false_indices=tuple(int(value) for value in node_indices[~true_mask]),
        )

    def _build_leaves(
        self,
        indices: tuple[int, ...],
        path: tuple[PredicateAtom, ...],
        depth: int,
    ) -> list[_Leaf]:
        labels = Counter(self.rows[index].label for index in indices)
        predicted, count = labels.most_common(1)[0]
        confidence = count / len(indices)
        if confidence == 1.0 or depth >= self.profile.induction.maximum_depth:
            return [_Leaf(path, indices, predicted, confidence)]
        split = self._best_split(indices)
        if split is None:
            return [_Leaf(path, indices, predicted, confidence)]
        return [
            *self._build_leaves(
                split.true_indices,
                (*path, split.atom),
                depth + 1,
            ),
            *self._build_leaves(
                split.false_indices,
                (*path, inverse_atom(split.atom)),
                depth + 1,
            ),
        ]

    @staticmethod
    def _rule_id(prefix: str, path: Sequence[PredicateAtom], label: Label) -> str:
        payload = repr((tuple(path), label)).encode("utf-8")
        return f"{prefix}-{hashlib.sha256(payload).hexdigest()[:12].upper()}"

    def _general_rules(self, leaves: Sequence[_Leaf]) -> list[DistilledRule]:
        rules: list[DistilledRule] = []
        ordered = sorted(
            leaves,
            key=lambda leaf: (
                -len(leaf.path),
                -leaf.confidence,
                -len(leaf.indices),
            ),
        )
        for index, leaf in enumerate(ordered):
            if (
                leaf.confidence < 1.0
                or len(leaf.indices)
                < self.profile.induction.minimum_general_support
            ):
                continue
            pair_ids = tuple(self.rows[row_index].pair.pair_id for row_index in leaf.indices)
            source_groups = tuple(
                sorted(
                    {
                        self.rows[row_index].pair.source_group
                        for row_index in leaf.indices
                    }
                )
            )
            rules.append(
                DistilledRule(
                    rule_id=self._rule_id(
                        f"DISTILLED-{self.profile.profile_id.upper()}-GENERAL",
                        leaf.path,
                        leaf.predicted,
                    ),
                    priority=100_000 + index,
                    predicates=leaf.path,
                    outputs=dict(leaf.predicted),
                    support=len(leaf.indices),
                    confidence=leaf.confidence,
                    source_groups=source_groups,
                    validation_accuracy=0.0,
                    kind="general",
                    evidence_ids=pair_ids,
                )
            )
        return rules

    @staticmethod
    def predict(
        features: Mapping[str, Any],
        rules: Sequence[DistilledRule],
    ) -> Mapping[str, Any] | None:
        for rule in sorted(rules, key=lambda item: item.priority):
            if all(evaluate_atom(atom, features) for atom in rule.predicates):
                return rule.outputs
        return None

    def _exception_predicates(
        self,
        row_index: int,
        competing_indices: Sequence[int],
    ) -> tuple[PredicateAtom, ...]:
        del competing_indices
        row = self.rows[row_index]
        full_features = row.features

        for identity_group in self._identity_labels:
            atoms: list[PredicateAtom] = []
            for field in identity_group:
                value = full_features.get(field)
                if not clean_text(value):
                    atoms = []
                    break
                atoms.append(PredicateAtom(field, "eq", value))
            if not atoms:
                continue
            key = tuple(normalized_key(full_features.get(field)) for field in identity_group)
            if self._identity_labels[identity_group].get(key) == frozenset({row.label}):
                return tuple(atoms)
        return (
            PredicateAtom(
                "__evidence_hash",
                "eq",
                full_features["__evidence_hash"],
            ),
        )

    def _exception_rules(
        self,
        general_rules: Sequence[DistilledRule],
    ) -> list[DistilledRule]:
        misclassified: list[int] = []
        for index, row in enumerate(self.rows):
            predicted = self.predict(row.features, general_rules)
            if predicted != dict(row.label):
                misclassified.append(index)
        exceptions: list[DistilledRule] = []
        grouped: dict[
            tuple[str, Label],
            dict[str, Any],
        ] = {}
        for index in misclassified:
            row = self.rows[index]
            selector_field = "__evidence_hash"
            selector_value = row.features[selector_field]
            for identity_group in self._identity_labels:
                if len(identity_group) != 1:
                    continue
                field = identity_group[0]
                value = row.features.get(field)
                if not clean_text(value):
                    continue
                key = (normalized_key(value),)
                if self._identity_labels[identity_group].get(key) == frozenset(
                    {row.label}
                ):
                    selector_field = field
                    selector_value = value
                    break
            bucket = grouped.setdefault(
                (selector_field, row.label),
                {"values": [], "indices": []},
            )
            bucket["values"].append(selector_value)
            bucket["indices"].append(index)

        for rule_index, ((field, label), bucket) in enumerate(grouped.items()):
            values = list(dict.fromkeys(bucket["values"]))
            indices = list(bucket["indices"])
            predicate = PredicateAtom(
                field,
                "eq" if len(values) == 1 else "in",
                values[0] if len(values) == 1 else values,
            )
            predicates = (predicate,)
            exceptions.append(
                DistilledRule(
                    rule_id=self._rule_id(
                        f"DISTILLED-{self.profile.profile_id.upper()}-EXCEPTION",
                        predicates,
                        label,
                    ),
                    priority=10_000 + rule_index,
                    predicates=predicates,
                    outputs=dict(label),
                    support=len(indices),
                    confidence=1.0,
                    source_groups=tuple(
                        sorted({self.rows[index].pair.source_group for index in indices})
                    ),
                    validation_accuracy=0.0,
                    kind="exception",
                    evidence_ids=tuple(
                        self.rows[index].pair.pair_id for index in indices
                    ),
                )
            )
        return exceptions

    def fit(
        self,
        rows: Sequence[ProjectedPair],
        *,
        include_exceptions: bool = True,
    ) -> tuple[DistilledRule, ...]:
        if not rows:
            return ()
        self.rows = rows
        self._prepare_indexes()
        leaves = self._build_leaves(tuple(range(len(rows))), (), 0)
        general_rules = self._general_rules(leaves)
        exceptions = (
            self._exception_rules(general_rules) if include_exceptions else []
        )
        return tuple(sorted([*exceptions, *general_rules], key=lambda rule: rule.priority))

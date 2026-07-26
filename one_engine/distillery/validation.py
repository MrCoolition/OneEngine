from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import replace
from typing import Any, Mapping, Sequence

from .induction import RuleInducer
from .models import (
    DistillationProfile,
    DistilledRule,
    ProjectedPair,
    ValidationResult,
)


def validate_rules(
    rows: Sequence[ProjectedPair],
    rules: Sequence[DistilledRule],
) -> ValidationResult:
    exact = 0
    matched = 0
    uncovered: list[str] = []
    mismatched: list[str] = []
    group_stats: dict[str, Counter[str]] = defaultdict(Counter)
    state_outputs: dict[tuple[tuple[str, Any], ...], set[tuple[tuple[str, Any], ...]]] = (
        defaultdict(set)
    )
    for row in rows:
        state = tuple(sorted((key, repr(value)) for key, value in row.features.items()))
        state_outputs[state].add(row.label)
        predicted = RuleInducer.predict(row.features, rules)
        stats = group_stats[row.pair.source_group]
        stats["rows"] += 1
        if predicted is None:
            uncovered.append(row.pair.pair_id)
            stats["uncovered"] += 1
            continue
        matched += 1
        stats["matched"] += 1
        if predicted == dict(row.label):
            exact += 1
            stats["exact"] += 1
        else:
            mismatched.append(row.pair.pair_id)
            stats["mismatched"] += 1
    row_count = len(rows)
    contradictions = sum(len(outputs) > 1 for outputs in state_outputs.values())
    by_source = {
        group: {
            **dict(stats),
            "accuracy": stats["exact"] / stats["rows"] if stats["rows"] else 0.0,
        }
        for group, stats in sorted(group_stats.items())
    }
    return ValidationResult(
        row_count=row_count,
        matched_count=matched,
        exact_count=exact,
        accuracy=exact / row_count if row_count else 0.0,
        contradictions=contradictions,
        uncovered_pair_ids=tuple(uncovered),
        mismatched_pair_ids=tuple(mismatched),
        by_source_group=by_source,
    )


def temporal_holdout_validation(
    rows: Sequence[ProjectedPair],
    profile: DistillationProfile,
) -> tuple[Mapping[str, Any], tuple[DistilledRule, ...]]:
    groups = sorted({row.pair.source_group for row in rows})
    fold_results: dict[str, Any] = {}
    stable_rule_scores: dict[str, list[float]] = defaultdict(list)
    for holdout in groups:
        training = [row for row in rows if row.pair.source_group != holdout]
        testing = [row for row in rows if row.pair.source_group == holdout]
        rules = RuleInducer(profile).fit(training, include_exceptions=False)
        result = validate_rules(testing, rules)
        fold_results[holdout] = {
            "training_rows": len(training),
            "testing_rows": len(testing),
            "rule_count": len(rules),
            "accuracy": result.accuracy,
            "exact": result.exact_count,
            "uncovered": len(result.uncovered_pair_ids),
            "mismatched": len(result.mismatched_pair_ids),
        }
        for rule in rules:
            stable_rule_scores[rule.rule_id].append(result.accuracy)

    full_general = RuleInducer(profile).fit(rows, include_exceptions=False)
    scored_rules = tuple(
        replace(
            rule,
            validation_accuracy=(
                sum(stable_rule_scores.get(rule.rule_id, ()))
                / len(stable_rule_scores[rule.rule_id])
                if stable_rule_scores.get(rule.rule_id)
                else 0.0
            ),
        )
        for rule in full_general
    )
    accuracies = [fold["accuracy"] for fold in fold_results.values()]
    summary = {
        "strategy": "leave-one-source-group-out",
        "folds": fold_results,
        "mean_accuracy": sum(accuracies) / len(accuracies) if accuracies else 0.0,
        "minimum_accuracy": min(accuracies) if accuracies else 0.0,
        "maximum_accuracy": max(accuracies) if accuracies else 0.0,
    }
    return summary, scored_rules

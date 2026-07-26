from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .models import (
    DistillationProfile,
    Document,
    MatchEvidence,
    MatchResult,
    RowPair,
    UnmatchedRow,
)
from .normalization import canonical_action, clean_text, normalized_key, stable_value


def _non_output_fields(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    profile: DistillationProfile,
) -> tuple[str, ...]:
    excluded = (
        profile.output_source_fields
        | profile.matching.ignored_fields
        | profile.matching.volatile_fields
    )
    return tuple(sorted((set(before) | set(after)) - excluded))


def _fingerprint(row: Mapping[str, Any], fields: Iterable[str]) -> str:
    payload = [(field, stable_value(row.get(field))) for field in sorted(fields)]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _identity_key(
    row: Mapping[str, Any],
    fields: Sequence[str],
) -> tuple[str, ...] | None:
    values = tuple(normalized_key(row.get(field)) for field in fields)
    return values if values and all(values) else None


def _changed_fields(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    profile: DistillationProfile,
) -> tuple[str, ...]:
    fields = _non_output_fields(before, after, profile)
    return tuple(
        field
        for field in fields
        if stable_value(before.get(field)) != stable_value(after.get(field))
    )


def _outputs(after: Mapping[str, Any], profile: DistillationProfile) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for output in profile.output_fields:
        value = after.get(output.source)
        values[output.target] = (
            canonical_action(value) if output.normalizer == "action" else clean_text(value)
        )
    return values


def _pair_id(
    source_group: str,
    before_index: int,
    after_index: int,
    before: Mapping[str, Any],
) -> str:
    digest = hashlib.sha256(
        (
            source_group
            + "|"
            + str(before_index)
            + "|"
            + str(after_index)
            + "|"
            + json.dumps(dict(before), default=str, sort_keys=True, ensure_ascii=False)
        ).encode("utf-8")
    ).hexdigest()
    return digest[:24]


def _similarity(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    fields: Sequence[str],
) -> float:
    comparable = 0
    matched = 0.0
    for field in fields:
        left = normalized_key(before.get(field))
        right = normalized_key(after.get(field))
        if not left and not right:
            continue
        comparable += 1
        if left == right:
            matched += 1.0
        elif left and right and (left in right or right in left):
            matched += 0.6
    return matched / comparable if comparable else 0.0


class RowMatcher:
    def __init__(self, profile: DistillationProfile):
        self.profile = profile

    def _make_pair(
        self,
        before_document: Document,
        after_document: Document,
        before_index: int,
        after_index: int,
        method: str,
        score: float,
        *,
        identity_fields: Sequence[str] = (),
        ambiguous: bool = False,
        notes: Sequence[str] = (),
    ) -> RowPair:
        before = before_document.rows[before_index]
        after = after_document.rows[after_index]
        return RowPair(
            pair_id=_pair_id(
                before_document.source_group,
                before_index,
                after_index,
                before,
            ),
            source_group=before_document.source_group,
            before_index=before_index,
            after_index=after_index,
            before=before,
            after=after,
            outputs=_outputs(after, self.profile),
            evidence=MatchEvidence(
                method=method,
                score=score,
                identity_fields=tuple(identity_fields),
                changed_input_fields=_changed_fields(before, after, self.profile),
                ambiguous=ambiguous,
                notes=tuple(notes),
            ),
        )

    def match_documents(
        self,
        before_document: Document,
        after_document: Document,
    ) -> MatchResult:
        if before_document.source_group != after_document.source_group:
            raise ValueError("Cannot match documents from different source groups")

        unmatched_before = set(range(len(before_document.rows)))
        unmatched_after = set(range(len(after_document.rows)))
        pairs: list[RowPair] = []

        all_fields = sorted(
            (
                set().union(*(row.keys() for row in before_document.rows))
                | set().union(*(row.keys() for row in after_document.rows))
            )
            - self.profile.output_source_fields
            - self.profile.matching.ignored_fields
            - self.profile.matching.volatile_fields
        )

        before_fingerprints: dict[str, deque[int]] = defaultdict(deque)
        after_fingerprints: dict[str, deque[int]] = defaultdict(deque)
        for index in unmatched_before:
            before_fingerprints[_fingerprint(before_document.rows[index], all_fields)].append(
                index
            )
        for index in unmatched_after:
            after_fingerprints[_fingerprint(after_document.rows[index], all_fields)].append(
                index
            )
        for fingerprint in sorted(set(before_fingerprints) & set(after_fingerprints)):
            before_queue = before_fingerprints[fingerprint]
            after_queue = after_fingerprints[fingerprint]
            while before_queue and after_queue:
                before_index = before_queue.popleft()
                after_index = after_queue.popleft()
                unmatched_before.discard(before_index)
                unmatched_after.discard(after_index)
                pairs.append(
                    self._make_pair(
                        before_document,
                        after_document,
                        before_index,
                        after_index,
                        "exact_payload",
                        1.0,
                    )
                )

        for identity_fields in self.profile.matching.identity_groups:
            before_index: dict[tuple[str, ...], list[int]] = defaultdict(list)
            after_index: dict[tuple[str, ...], list[int]] = defaultdict(list)
            for index in unmatched_before:
                key = _identity_key(before_document.rows[index], identity_fields)
                if key:
                    before_index[key].append(index)
            for index in unmatched_after:
                key = _identity_key(after_document.rows[index], identity_fields)
                if key:
                    after_index[key].append(index)
            for key in sorted(set(before_index) & set(after_index)):
                left = before_index[key]
                right = after_index[key]
                if len(left) != 1 or len(right) != 1:
                    continue
                before_row_index = left[0]
                after_row_index = right[0]
                unmatched_before.discard(before_row_index)
                unmatched_after.discard(after_row_index)
                pairs.append(
                    self._make_pair(
                        before_document,
                        after_document,
                        before_row_index,
                        after_row_index,
                        "unique_identity",
                        0.98,
                        identity_fields=identity_fields,
                    )
                )

        similarity_fields = (
            self.profile.matching.similarity_fields
            or tuple(
                field
                for field in all_fields
                if field not in self.profile.matching.volatile_fields
            )
        )
        proposals: list[tuple[float, float, int, int]] = []
        for before_index in unmatched_before:
            scores = sorted(
                (
                    (
                        _similarity(
                            before_document.rows[before_index],
                            after_document.rows[after_index],
                            similarity_fields,
                        ),
                        after_index,
                    )
                    for after_index in unmatched_after
                ),
                reverse=True,
            )
            if not scores:
                continue
            best_score, _ = scores[0]
            second_score = scores[1][0] if len(scores) > 1 else 0.0
            if best_score < self.profile.matching.minimum_similarity:
                continue
            margin = best_score - second_score
            for score, after_index in scores:
                if score < self.profile.matching.minimum_similarity:
                    break
                proposals.append((score, margin, before_index, after_index))

        for score, margin, before_index, after_index in sorted(
            proposals,
            key=lambda item: (item[0], item[1]),
            reverse=True,
        ):
            if before_index not in unmatched_before or after_index not in unmatched_after:
                continue
            ambiguous = margin < self.profile.matching.ambiguity_margin
            unmatched_before.remove(before_index)
            unmatched_after.remove(after_index)
            pairs.append(
                self._make_pair(
                    before_document,
                    after_document,
                    before_index,
                    after_index,
                    "similarity",
                    score,
                    ambiguous=ambiguous,
                    notes=(f"best-to-second margin={margin:.4f}",),
                )
            )

        unmatched: list[UnmatchedRow] = []
        for index in sorted(unmatched_before):
            unmatched.append(
                UnmatchedRow(
                    side="before",
                    source_group=before_document.source_group,
                    row_index=index,
                    row=before_document.rows[index],
                    reason="no after row met the configured identity or similarity threshold",
                )
            )
        for index in sorted(unmatched_after):
            unmatched.append(
                UnmatchedRow(
                    side="after",
                    source_group=after_document.source_group,
                    row_index=index,
                    row=after_document.rows[index],
                    reason="no before row met the configured identity or similarity threshold",
                )
            )

        pairs.sort(key=lambda pair: pair.before_index)
        method_counts = Counter(pair.evidence.method for pair in pairs)
        return MatchResult(
            pairs=tuple(pairs),
            unmatched=tuple(unmatched),
            method_counts=dict(method_counts),
        )


def match_document_sets(
    before_documents: Sequence[Document],
    after_documents: Sequence[Document],
    profile: DistillationProfile,
) -> MatchResult:
    def index_documents(
        documents: Sequence[Document],
        side: str,
    ) -> dict[str, Document]:
        indexed: dict[str, Document] = {}
        for document in documents:
            if document.source_group in indexed:
                raise ValueError(
                    f"Duplicate {side} source group {document.source_group!r}: "
                    f"{indexed[document.source_group].name!r} and "
                    f"{document.name!r}. Configure distinct pairing groups."
                )
            indexed[document.source_group] = document
        return indexed

    before_by_group = index_documents(before_documents, "before")
    after_by_group = index_documents(after_documents, "after")
    missing_after = sorted(set(before_by_group) - set(after_by_group))
    missing_before = sorted(set(after_by_group) - set(before_by_group))
    if missing_after or missing_before:
        raise ValueError(
            f"Unpaired source groups: missing after={missing_after}, "
            f"missing before={missing_before}"
        )
    matcher = RowMatcher(profile)
    pairs: list[RowPair] = []
    unmatched: list[UnmatchedRow] = []
    methods: Counter[str] = Counter()
    for group in sorted(before_by_group):
        result = matcher.match_documents(before_by_group[group], after_by_group[group])
        pairs.extend(result.pairs)
        unmatched.extend(result.unmatched)
        methods.update(result.method_counts)
    return MatchResult(
        pairs=tuple(pairs),
        unmatched=tuple(unmatched),
        method_counts=dict(methods),
    )

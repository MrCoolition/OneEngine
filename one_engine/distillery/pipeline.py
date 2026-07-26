from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .adapters import documents_from_path
from .emitter import build_catalog, build_snowflake_merge_sql
from .features import project_pairs
from .induction import RuleInducer
from .matching import match_document_sets
from .models import DistillationProfile, DistillationRun
from .profiles import load_profile
from .validation import temporal_holdout_validation, validate_rules


DISTILLERY_VERSION = "1.2.0"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_sha256(path: Path) -> str:
    if path.is_file():
        return file_sha256(path)
    if not path.is_dir():
        raise FileNotFoundError(f"Source path not found: {path}")
    digest = hashlib.sha256()
    for candidate in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = candidate.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_sha256(candidate).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def _run_id(
    profile: DistillationProfile,
    before_path: Path,
    after_path: Path,
    *,
    run_holdouts: bool,
) -> str:
    payload = "|".join(
        [
            DISTILLERY_VERSION,
            profile.profile_id,
            profile.version,
            "holdouts" if run_holdouts else "draft",
            source_sha256(before_path),
            source_sha256(after_path),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def distill(
    *,
    profile: str | Path | DistillationProfile,
    before_path: str | Path,
    after_path: str | Path,
    run_holdouts: bool = True,
) -> DistillationRun:
    resolved_profile = (
        profile if isinstance(profile, DistillationProfile) else load_profile(profile)
    )
    before = Path(before_path).resolve()
    after = Path(after_path).resolve()
    before_documents = documents_from_path(before, resolved_profile)
    after_documents = documents_from_path(after, resolved_profile)
    match_result = match_document_sets(
        before_documents,
        after_documents,
        resolved_profile,
    )
    projected = project_pairs(match_result.pairs, resolved_profile)
    inducer = RuleInducer(resolved_profile)
    rules = inducer.fit(projected, include_exceptions=True)
    validation = validate_rules(projected, rules)
    if run_holdouts:
        holdout, _ = temporal_holdout_validation(projected, resolved_profile)
    else:
        holdout = {
            "strategy": "not-run",
            "folds": {},
            "mean_accuracy": 0.0,
            "minimum_accuracy": 0.0,
            "maximum_accuracy": 0.0,
        }
    holdout_accuracy = float(holdout.get("mean_accuracy") or 0.0)
    scored_rules = tuple(
        replace(
            rule,
            validation_accuracy=(
                holdout_accuracy if rule.kind == "general" else 0.0
            ),
        )
        for rule in rules
    )
    changed_fields = Counter(
        field
        for pair in match_result.pairs
        for field in pair.evidence.changed_input_fields
    )
    label_counts = Counter(
        json.dumps(dict(row.label), sort_keys=True, ensure_ascii=False)
        for row in projected
    )
    deployment_eligible = (
        validation.accuracy == 1.0
        and validation.contradictions == 0
        and not match_result.unmatched
    )
    diagnostics: dict[str, Any] = {
        "distillery_version": DISTILLERY_VERSION,
        "source": {
            "before": {
                "path": str(before),
                "sha256": source_sha256(before),
                "documents": len(before_documents),
                "rows": sum(len(document.rows) for document in before_documents),
            },
            "after": {
                "path": str(after),
                "sha256": source_sha256(after),
                "documents": len(after_documents),
                "rows": sum(len(document.rows) for document in after_documents),
            },
        },
        "matching": {
            "pairs": len(match_result.pairs),
            "unmatched": len(match_result.unmatched),
            "ambiguous": sum(
                pair.evidence.ambiguous for pair in match_result.pairs
            ),
            "methods": dict(match_result.method_counts),
            "changed_input_fields": dict(changed_fields.most_common()),
        },
        "labels": {
            "unique": len(label_counts),
            "counts": {
                key: count for key, count in label_counts.most_common()
            },
        },
        "rules": {
            "total": len(scored_rules),
            "general": sum(rule.kind == "general" for rule in scored_rules),
            "exception": sum(rule.kind == "exception" for rule in scored_rules),
            "general_support": sum(
                rule.support for rule in scored_rules if rule.kind == "general"
            ),
            "exception_support": sum(
                rule.support for rule in scored_rules if rule.kind == "exception"
            ),
        },
        "holdout": holdout,
        "deployment_gate": {
            "eligible": deployment_eligible,
            "requirements": {
                "corpus_accuracy": 1.0,
                "unmatched_rows": 0,
                "contradictions": 0,
            },
            "observed": {
                "corpus_accuracy": validation.accuracy,
                "unmatched_rows": len(match_result.unmatched),
                "contradictions": validation.contradictions,
            },
        },
    }
    return DistillationRun(
        run_id=_run_id(
            resolved_profile,
            before,
            after,
            run_holdouts=run_holdouts,
        ),
        profile_id=resolved_profile.profile_id,
        profile_version=resolved_profile.version,
        created_at=datetime.now(timezone.utc).isoformat(),
        source_paths=(before, after),
        match_result=match_result,
        rules=scored_rules,
        validation=validation,
        diagnostics=diagnostics,
    )


def _report_payload(run: DistillationRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "profile_id": run.profile_id,
        "profile_version": run.profile_version,
        "created_at": run.created_at,
        "source_paths": [str(path) for path in run.source_paths],
        "validation": asdict(run.validation),
        "diagnostics": run.diagnostics,
    }


def save_run(
    run: DistillationRun,
    profile: DistillationProfile,
    output_dir: str | Path,
) -> dict[str, Path]:
    output = Path(output_dir)
    run_dir = output / "runs" / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    current_manifest = output / "current.json"
    previous_manifest: dict[str, Any] = {}
    if current_manifest.exists():
        try:
            previous_manifest = json.loads(
                current_manifest.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            previous_manifest = {}
    holdout_accuracy = float(
        (run.diagnostics.get("holdout") or {}).get("mean_accuracy") or 0.0
    )
    deployment_eligible = bool(
        (run.diagnostics.get("deployment_gate") or {}).get("eligible")
    )
    catalog = build_catalog(
        run.rules,
        profile,
        run_id=run.run_id,
        holdout_accuracy=holdout_accuracy,
        deployment_eligible=deployment_eligible,
    )
    report_path = run_dir / "report.json"
    catalog_path = run_dir / "catalog.json"
    sql_path = run_dir / "load_snowflake.sql"
    if not report_path.exists():
        report_path.write_text(
            json.dumps(_report_payload(run), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    if not catalog_path.exists():
        catalog_path.write_text(
            json.dumps(catalog, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    if not sql_path.exists():
        sql_path.write_text(
            build_snowflake_merge_sql(catalog),
            encoding="utf-8",
        )
    previous_run_id = str(previous_manifest.get("run_id") or "")
    current_manifest.write_text(
        json.dumps(
            {
                "run_id": run.run_id,
                "profile_id": run.profile_id,
                "profile_version": run.profile_version,
                "report": str(report_path.relative_to(output)),
                "catalog": str(catalog_path.relative_to(output)),
                "snowflake_sql": str(sql_path.relative_to(output)),
                "validation_accuracy": run.validation.accuracy,
                "holdout_accuracy": holdout_accuracy,
                "deployment_eligible": deployment_eligible,
                "previous_run_id": (
                    previous_run_id
                    if previous_run_id and previous_run_id != run.run_id
                    else None
                ),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    history_path = output / "history.json"
    history: list[dict[str, Any]] = []
    if history_path.exists():
        try:
            loaded_history = json.loads(history_path.read_text(encoding="utf-8"))
            if isinstance(loaded_history, list):
                history = [
                    dict(item)
                    for item in loaded_history
                    if isinstance(item, Mapping)
                ]
        except (OSError, json.JSONDecodeError):
            history = []
    history_entry = {
        "run_id": run.run_id,
        "created_at": run.created_at,
        "profile_id": run.profile_id,
        "profile_version": run.profile_version,
        "source_paths": [str(path) for path in run.source_paths],
        "row_count": run.validation.row_count,
        "rule_count": len(run.rules),
        "validation_accuracy": run.validation.accuracy,
        "holdout_accuracy": holdout_accuracy,
        "deployment_eligible": deployment_eligible,
    }
    history = [
        item for item in history if str(item.get("run_id")) != run.run_id
    ]
    history.append(history_entry)
    history_path.write_text(
        json.dumps(history, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {
        "manifest": current_manifest,
        "history": history_path,
        "report": report_path,
        "catalog": catalog_path,
        "snowflake_sql": sql_path,
    }

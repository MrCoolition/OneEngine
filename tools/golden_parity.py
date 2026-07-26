from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import sys
import zipfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_APP = ROOT / "app" / "streamlit_app.py"
DEFAULT_BEFORE = ROOT / "before.zip"
DEFAULT_AFTER = ROOT / "after.zip"
DECISION_COLUMNS = {"ACTION", "If In Stock: Action", "Buysmart Action"}


def load_app(path: Path):
    spec = importlib.util.spec_from_file_location("one_engine_parity_app", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def source_key(app: Any, row: dict[str, Any]) -> str:
    collapsed = app.collapse_raw_row(row)
    values = [
        (key, app.clean_text(value))
        for key, value in sorted(collapsed.items())
        if key not in DECISION_COLUMNS
    ]
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def semantic_action(app: Any, value: Any) -> str:
    key = app.normalize_key(value)
    aliases = {
        "FIND ALT 1ST": "FIND ALT FIRST",
        "APPROVED 1 X": "APPROVED 1X",
    }
    return aliases.get(key, key)


def ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def workbook_entries(archive: zipfile.ZipFile) -> dict[str, str]:
    return {
        Path(name).name: name
        for name in archive.namelist()
        if name.lower().endswith((".xlsx", ".xlsm"))
    }


def compare_archives(app: Any, before_path: Path, after_path: Path) -> dict[str, Any]:
    rules, catalog_report = app.build_seed_catalog()
    totals: collections.Counter[str] = collections.Counter()
    expected_actions: collections.Counter[str] = collections.Counter()
    actual_actions: collections.Counter[str] = collections.Counter()
    expected_stock_actions: collections.Counter[str] = collections.Counter()
    actual_stock_actions: collections.Counter[str] = collections.Counter()
    files: list[dict[str, Any]] = []

    with zipfile.ZipFile(before_path) as before_zip, zipfile.ZipFile(after_path) as after_zip:
        before_entries = workbook_entries(before_zip)
        after_entries = workbook_entries(after_zip)
        missing_after = sorted(set(before_entries) - set(after_entries))
        if missing_after:
            raise ValueError(f"Missing after workbooks: {', '.join(missing_after)}")

        for file_name in sorted(before_entries):
            before = app.parse_source_workbook(
                file_name,
                before_zip.read(before_entries[file_name]),
            )
            after = app.parse_source_workbook(
                file_name,
                after_zip.read(after_entries[file_name]),
            )

            expected_by_source: dict[str, collections.deque[dict[str, Any]]] = (
                collections.defaultdict(collections.deque)
            )
            for row in after.rows:
                expected_by_source[source_key(app, row)].append(row)

            matched: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
            unmatched_before = 0
            for index, row in enumerate(before.rows):
                candidates = expected_by_source.get(source_key(app, row))
                if not candidates:
                    unmatched_before += 1
                    continue
                matched.append((index, row, candidates.popleft()))

            unmatched_after = sum(len(queue) for queue in expected_by_source.values())
            row_numbers = before.source_row_numbers or [
                index + 2 for index in range(len(before.rows))
            ]
            workflow_rows = [
                app.create_workflow_row(
                    "golden-parity",
                    source_row,
                    row_numbers[index],
                    now="2026-07-26T00:00:00+00:00",
                )
                for index, source_row, _ in matched
            ]
            actual_rows, _, _ = app.execute_rows(
                workflow_rows,
                rules,
                reference_lists=app.DEFAULT_REFERENCE_LISTS,
            )

            metrics: collections.Counter[str] = collections.Counter(
                {
                    "before_rows": len(before.rows),
                    "after_rows": len(after.rows),
                    "matched_rows": len(matched),
                    "unmatched_before": unmatched_before,
                    "unmatched_after": unmatched_after,
                }
            )

            for actual, (_, _, expected_raw) in zip(actual_rows, matched, strict=True):
                expected = app.collapse_raw_row(expected_raw)
                expected_action = app.normalize_action(expected.get("ACTION"))
                actual_action = app.normalize_action(actual.get("action"))
                expected_stock = app.normalize_action(expected.get("If In Stock: Action"))
                actual_stock = app.normalize_action(actual.get("if_in_stock_action"))

                action_exact = expected_action == actual_action
                stock_exact = expected_stock == actual_stock
                action_semantic = semantic_action(app, expected_action) == semantic_action(
                    app, actual_action
                )
                stock_semantic = semantic_action(app, expected_stock) == semantic_action(
                    app, actual_stock
                )

                metrics["action_exact"] += action_exact
                metrics["stock_exact"] += stock_exact
                metrics["action_semantic"] += action_semantic
                metrics["stock_semantic"] += stock_semantic
                metrics["joint_semantic"] += action_semantic and stock_semantic
                expected_actions[expected_action or "<blank>"] += 1
                actual_actions[actual_action or "<blank>"] += 1
                expected_stock_actions[expected_stock or "<blank>"] += 1
                actual_stock_actions[actual_stock or "<blank>"] += 1

            totals.update(metrics)
            files.append({"file": file_name, **dict(metrics)})

    matched_rows = totals["matched_rows"]
    return {
        "catalog": {
            "rules": len(rules),
            "variants": sum(len(rule.get("variants") or []) for rule in rules),
            "executable_variants": catalog_report["executableVariants"],
            "guided_variants": catalog_report["guidedVariants"],
            "manual_variants": catalog_report["manualVariants"],
        },
        "files": files,
        "totals": dict(totals),
        "rates": {
            "action_exact": ratio(totals["action_exact"], matched_rows),
            "stock_exact": ratio(totals["stock_exact"], matched_rows),
            "action_semantic": ratio(totals["action_semantic"], matched_rows),
            "stock_semantic": ratio(totals["stock_semantic"], matched_rows),
            "joint_semantic": ratio(totals["joint_semantic"], matched_rows),
        },
        "distributions": {
            "expected_action": expected_actions.most_common(),
            "actual_action": actual_actions.most_common(),
            "expected_if_in_stock": expected_stock_actions.most_common(),
            "actual_if_in_stock": actual_stock_actions.most_common(),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ONE ENGINE decisions with paired golden workbooks."
    )
    parser.add_argument("--app", type=Path, default=DEFAULT_APP)
    parser.add_argument("--before", type=Path, default=DEFAULT_BEFORE)
    parser.add_argument("--after", type=Path, default=DEFAULT_AFTER)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = compare_archives(load_app(args.app), args.before, args.after)
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path

from one_engine.distillery.adapters import documents_from_path
from one_engine.distillery.emitter import build_catalog, build_snowflake_merge_sql
from one_engine.distillery.features import evidence_hash
from one_engine.distillery.normalization import canonical_action
from one_engine.distillery.pipeline import distill
from one_engine.distillery.profiles import load_profile


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"


def load_app():
    spec = importlib.util.spec_from_file_location(
        "one_engine_distillery_contract_app",
        APP_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ProductRequestDistilleryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = load_profile("product_request")
        cls.distilled_run = distill(
            profile=cls.profile,
            before_path=ROOT / "before.zip",
            after_path=ROOT / "after.zip",
            run_holdouts=False,
        )
        cls.catalog = build_catalog(
            cls.distilled_run.rules,
            cls.profile,
            run_id=cls.distilled_run.run_id,
            holdout_accuracy=0.0,
        )
        cls.app = load_app()

    def test_corpus_alignment_is_complete(self) -> None:
        self.assertEqual(6922, len(self.distilled_run.match_result.pairs))
        self.assertEqual(0, len(self.distilled_run.match_result.unmatched))
        self.assertEqual(1.0, self.distilled_run.validation.accuracy)
        self.assertEqual(0, self.distilled_run.validation.contradictions)

    def test_catalog_is_compact_and_executable(self) -> None:
        self.assertLessEqual(len(self.catalog), 200)
        self.assertTrue(self.catalog)
        for rule in self.catalog:
            variant = rule["variants"][0]
            self.assertTrue(variant["enabled"])
            self.assertTrue(variant["is_executable"])
            self.assertTrue(variant["stop_processing"])
            self.assertTrue(variant["action_json"])
            source = rule["source"]
            if source["distilled_rule_kind"] == "general":
                self.assertEqual(1.0, source["confidence"])
                self.assertGreaterEqual(source["support"], 3)

    def test_streamlit_runtime_executes_distilled_catalog_at_full_parity(self) -> None:
        rows = [
            self.app.create_workflow_row(
                "distillery-contract",
                pair.before,
                index + 2,
            )
            for index, pair in enumerate(self.distilled_run.match_result.pairs)
        ]
        first_context = self.app.context_for_row(rows[0])
        self.assertEqual(
            evidence_hash(self.distilled_run.match_result.pairs[0].before),
            first_context["__evidence_hash"],
        )

        executed, _, _ = self.app.execute_rows(rows, self.catalog)
        mismatches: list[tuple[str, dict[str, str], dict[str, str]]] = []
        for pair, row in zip(self.distilled_run.match_result.pairs, executed):
            expected = {
                key: canonical_action(value)
                for key, value in pair.outputs.items()
            }
            actual = {
                "action": canonical_action(row.get("action")),
                "if_in_stock_action": canonical_action(
                    row.get("if_in_stock_action")
                ),
            }
            if actual != expected:
                mismatches.append((pair.pair_id, expected, actual))
        self.assertEqual([], mismatches[:10])

    def test_source_loader_accepts_archives(self) -> None:
        documents = documents_from_path(ROOT / "before.zip", self.profile)
        self.assertEqual(10, len(documents))
        self.assertEqual(6922, sum(len(document.rows) for document in documents))

    def test_snowflake_loader_is_scoped_and_round_trips_catalog(self) -> None:
        sql = build_snowflake_merge_sql(self.catalog)
        sections = sql.split("\n$$\n")
        self.assertEqual(3, len(sections))
        embedded_catalog = json.loads(sections[1])
        self.assertEqual(len(self.catalog), len(embedded_catalog))
        self.assertIn("STATUS = 'retired'", sections[0])
        self.assertNotIn("$ONE_ENGINE_CATALOG$", sql)
        for rule in embedded_catalog:
            predicates = rule["variants"][0]["predicate_json"]["all"]
            self.assertEqual("__ruleset_id", predicates[0]["field"])
            self.assertEqual("product_request", predicates[0]["value"])


if __name__ == "__main__":
    unittest.main()

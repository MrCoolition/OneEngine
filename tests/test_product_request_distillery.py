from __future__ import annotations

import importlib.util
import sys
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"
BEFORE_PATH = ROOT / "before.zip"
AFTER_PATH = ROOT / "after.zip"


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
        cls.app = load_app()
        cls.profile = cls.app.distillery_profile("product_request")
        cls.before_bytes = BEFORE_PATH.read_bytes()
        cls.after_bytes = AFTER_PATH.read_bytes()
        cls.before_documents = cls.app.distillery_documents_from_upload(
            BEFORE_PATH.name,
            cls.before_bytes,
            cls.profile,
        )
        cls.after_documents = cls.app.distillery_documents_from_upload(
            AFTER_PATH.name,
            cls.after_bytes,
            cls.profile,
        )
        cls.pairs, cls.unmatched = cls.app.distillery_match_documents(
            cls.before_documents,
            cls.after_documents,
            cls.profile,
        )
        cls.result = cls.app.run_rules_distillery(
            profile_id="product_request",
            before_file_name=BEFORE_PATH.name,
            before_bytes=cls.before_bytes,
            after_file_name=AFTER_PATH.name,
            after_bytes=cls.after_bytes,
            run_holdouts=False,
        )
        cls.catalog = cls.result["catalog"]

    def test_corpus_alignment_is_complete(self) -> None:
        report = self.result["report"]
        self.assertEqual(6920, len(self.pairs))
        self.assertEqual((), self.unmatched)
        self.assertEqual(6920, report["matching"]["pairs"])
        self.assertEqual(0, report["matching"]["unmatched"])
        self.assertEqual(1.0, report["validation"]["accuracy"])
        self.assertEqual(0, report["validation"]["contradictions"])
        self.assertTrue(self.result["deployment_eligible"])

    def test_blank_spreadsheet_rows_are_not_evidence(self) -> None:
        self.assertEqual(
            6920,
            sum(document["row_count"] for document in self.before_documents),
        )
        self.assertEqual(
            6920,
            sum(document["row_count"] for document in self.after_documents),
        )
        self.assertTrue(
            all(
                any(self.app.clean_text(value) for value in pair.before.values())
                for pair in self.pairs
            )
        )

    def test_single_xlsx_is_not_mistaken_for_a_zip_collection(self) -> None:
        with zipfile.ZipFile(BEFORE_PATH) as archive:
            member = next(
                name
                for name in archive.namelist()
                if name.endswith("06_12_2026.xlsx")
            )
            documents = self.app.distillery_documents_from_upload(
                "PRF_SORF_SRF_06_12_2026.xlsx",
                archive.read(member),
                self.profile,
            )
        self.assertEqual(1, len(documents))
        self.assertEqual(662, documents[0]["row_count"])

    def test_catalog_is_compact_scoped_and_executable(self) -> None:
        self.assertEqual(169, len(self.catalog))
        for rule in self.catalog:
            variant = rule["variants"][0]
            self.assertTrue(variant["enabled"])
            self.assertTrue(variant["is_executable"])
            self.assertTrue(variant["stop_processing"])
            self.assertTrue(variant["action_json"])
            predicates = variant["predicate_json"]["all"]
            self.assertEqual("__ruleset_id", predicates[0]["field"])
            self.assertEqual("product_request", predicates[0]["value"])
            source = rule["source"]
            self.assertEqual("rules_distillery", source["kind"])
            if source["distilled_rule_kind"] == "general":
                self.assertEqual(1.0, source["confidence"])
                self.assertGreaterEqual(source["support"], 3)

    def test_streamlit_runtime_executes_catalog_at_full_parity(self) -> None:
        rows = [
            self.app.create_workflow_row(
                "distillery-contract",
                pair.before,
                index + 2,
            )
            for index, pair in enumerate(self.pairs)
        ]
        first_context = self.app.context_for_row(rows[0])
        self.assertEqual(
            self.app.distillery_evidence_hash(self.pairs[0].before),
            first_context["__evidence_hash"],
        )

        executed, _, _ = self.app.execute_rows(rows, self.catalog)
        mismatches = []
        for pair, row in zip(self.pairs, executed):
            expected = {
                key: self.app.distillery_action(value)
                for key, value in pair.outputs.items()
            }
            actual = {
                "action": self.app.distillery_action(row.get("action")),
                "if_in_stock_action": self.app.distillery_action(
                    row.get("if_in_stock_action")
                ),
            }
            if actual != expected:
                mismatches.append((pair.pair_id, expected, actual))
        self.assertEqual([], mismatches[:10])

    def test_snowflake_is_the_catalog_system_of_record(self) -> None:
        source = APP_PATH.read_text(encoding="utf-8")
        self.assertNotIn("from " + "one_engine", source)
        self.assertNotIn("import " + "one_engine", source)
        self.assertEqual([], list((ROOT / "one_engine").rglob("*.py")))
        self.assertEqual([], list((ROOT / "catalogs").rglob("*.*")))
        self.assertTrue(hasattr(self.app, "SingleFileRuleInducer"))
        self.assertTrue(
            hasattr(
                self.app.SnowflakeRulesStore,
                "promote_distilled_catalog",
            )
        )


if __name__ == "__main__":
    unittest.main()

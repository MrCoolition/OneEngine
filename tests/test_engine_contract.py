from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"


def load_app():
    spec = importlib.util.spec_from_file_location("one_engine_contract_app", APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class OneEngineContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = load_app()

    def test_brand_identity(self) -> None:
        self.assertEqual("ONE ENGINE", self.app.APP_TITLE)
        self.assertIn("one-engine", self.app.APP_VERSION)

    def test_foodbuy_design_foundations_contract(self) -> None:
        captured: list[str] = []

        class FakeStreamlit:
            @staticmethod
            def markdown(value, **_kwargs):
                captured.append(value)

        original_streamlit = self.app.st
        try:
            self.app.st = FakeStreamlit()
            self.app.app_styles()
        finally:
            self.app.st = original_streamlit

        css = "\n".join(captured)
        self.assertIn("--fb-primary-500: #7D36C9", css)
        self.assertIn("--fb-neutral-100: #DEE1E6", css)
        self.assertIn('--fb-font: "DM Sans", "Inter"', css)
        self.assertIn("--fb-radius-sm: 8px", css)
        self.assertIn("--fb-radius-md: 12px", css)
        self.assertIn("--fb-shadow-100:", css)
        self.assertIn("min-height: 44px", css)
        self.assertIn("prefers-reduced-motion", css)
        self.assertIn('[data-testid="stHeader"]', css)
        self.assertIn('[data-testid="stSidebarCollapsedControl"]', css)
        self.assertIn("background: rgba(247, 248, 249, .98)", css)
        self.assertIn("ONE_ENGINE_FOODBUY_DESIGN_SYSTEM", self.app.DEPLOYMENT_SENTINEL)

    def test_brand_asset_resolution_uses_snowflake_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = Path(directory) / "oneengine_brand.png"
            expected.write_bytes(b"test-brand")
            with patch.object(self.app.os, "getcwd", return_value=directory):
                resolved = self.app.one_engine_brand_image_path()

        self.assertEqual(str(expected.resolve()), resolved)
        source = APP_PATH.read_text(encoding="utf-8")
        self.assertIn('logo_renderer(brand_image, icon_image=brand_image)', source)

    def test_embedded_catalog_shape(self) -> None:
        rules, report = self.app.build_seed_catalog()

        self.assertEqual(53, len(rules))
        self.assertEqual(59, sum(len(rule.get("variants") or []) for rule in rules))
        self.assertEqual(32, report["executableVariants"])
        self.assertEqual(4, report["guidedVariants"])
        self.assertEqual(23, report["manualVariants"])
        self.assertEqual([], report["warnings"])

    def test_application_self_check(self) -> None:
        result = self.app.run_application_self_check()

        self.assertEqual("passed", result["status"])
        self.assertEqual(0, result["tests_failed"])
        self.assertGreaterEqual(result["tests_passed"], 5)

    def test_snowflake_table_contract(self) -> None:
        self.assertEqual(
            {
                "batches": "BATCHES",
                "rows": "WORKFLOW_ROWS",
                "rules": "RULES",
                "runs": "RUNS",
                "results": "ROW_RESULTS",
                "audit": "AUDIT_EVENTS",
                "references": "REFERENCE_LISTS",
            },
            self.app.TABLE_SUFFIXES,
        )

    def test_live_product_request_source_contract(self) -> None:
        app = self.app

        class FakeStore(app.SnowflakeRulesStore):
            def __init__(self, rows):
                super().__init__(object())
                self.source_rows = list(rows)
                self.queries = []

            def collect(self, query, params=None):
                self.queries.append((query, params))
                return list(self.source_rows)

        rows = [
            {
                "BUSINESS": "Compass USA",
                "REQUEST_TYPE": "SORF",
                "CASE_NUMBER": "WO-200",
                "CREATED_DATE": "2026-07-26",
                "ACTION": "Review",
            },
            {
                "BUSINESS": "Compass Canada",
                "REQUEST_TYPE": "PRF",
                "CASE_NUMBER": "WO-100",
                "CREATED_DATE": "2026-07-25",
                "ACTION": "Approved",
            },
        ]
        store = FakeStore(rows)
        parsed, source_hash, metadata = store.load_live_product_request_data()

        self.assertEqual("V_OE_PRODUCTREQUESTS", app.LIVE_PRODUCT_REQUEST_VIEW)
        self.assertEqual(
            'SELECT * FROM "FOODBUY_MASALA_PROD"."COMPLIANCE_LAB"."V_OE_PRODUCTREQUESTS"',
            store.queries[0][0],
        )
        self.assertEqual(2, len(parsed.rows))
        self.assertEqual("Snowflake live view", parsed.sheet_name)
        self.assertIn("Business", parsed.columns)
        self.assertIn("Type", parsed.columns)
        self.assertIn("Case#", parsed.columns)
        self.assertIn("Date Created", parsed.columns)
        self.assertEqual(64, len(source_hash))
        self.assertEqual(
            "FOODBUY_MASALA_PROD.COMPLIANCE_LAB.V_OE_PRODUCTREQUESTS",
            metadata["source_view"],
        )
        normalized = [
            app.create_normalized_row(row)
            for row in parsed.rows
        ]
        self.assertEqual(
            {"PRF", "SORF"},
            {row["fields"]["requestType"] for row in normalized},
        )
        self.assertEqual(
            {"WO-100", "WO-200"},
            {row["fields"]["caseNumber"] for row in normalized},
        )

        reversed_store = FakeStore(reversed(rows))
        _, reversed_hash, _ = reversed_store.load_live_product_request_data()
        self.assertEqual(source_hash, reversed_hash)

        source = APP_PATH.read_text(encoding="utf-8")
        self.assertIn('"Use Live Product Request Data"', source)
        self.assertIn(
            'source_kind = "SNOWFLAKE_VIEW"',
            source,
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


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
        self.assertIn("ONE_ENGINE_FOODBUY_DESIGN_SYSTEM", self.app.DEPLOYMENT_SENTINEL)

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


if __name__ == "__main__":
    unittest.main()

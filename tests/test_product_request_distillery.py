from __future__ import annotations

import importlib.util
import sys
import unittest
import zipfile
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"
BEFORE_PATH = ROOT / "before.zip"
AFTER_PATH = ROOT / "after.zip"


def load_app():
    spec = importlib.util.spec_from_file_location(
        "one_engine_literal_distillery_contract_app",
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
        cls.before_documents = cls.app.distillery_documents_from_upload(
            BEFORE_PATH.name,
            BEFORE_PATH.read_bytes(),
            cls.profile,
        )
        cls.after_documents = cls.app.distillery_documents_from_upload(
            AFTER_PATH.name,
            AFTER_PATH.read_bytes(),
            cls.profile,
        )
        cls.pairs, cls.unmatched = cls.app.distillery_match_documents(
            cls.before_documents,
            cls.after_documents,
            cls.profile,
        )

    def test_all_ten_dated_pairs_and_6920_rows_align(self) -> None:
        self.assertEqual(10, len(self.before_documents))
        self.assertEqual(10, len(self.after_documents))
        self.assertEqual(6920, len(self.pairs))
        self.assertEqual((), self.unmatched)
        self.assertEqual(
            6920,
            sum(item["row_count"] for item in self.before_documents),
        )
        self.assertEqual(
            6920,
            sum(item["row_count"] for item in self.after_documents),
        )

    def test_single_xlsx_is_not_mistaken_for_zip_collection(self) -> None:
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

    def test_atomic_output_contract_includes_audit_action(self) -> None:
        output_contract = {
            item["target"]: item["action_type"]
            for item in self.profile["output_fields"]
        }
        self.assertEqual(
            {
                "action": "set_action",
                "if_in_stock_action": "set_if_stock",
                "audit_action": "set_audit_action",
            },
            output_contract,
        )
        self.assertTrue(
            all(
                set(pair.outputs)
                == {"action", "if_in_stock_action", "audit_action"}
                for pair in self.pairs
            )
        )
        self.assertTrue(
            any(pair.outputs["audit_action"] for pair in self.pairs)
        )

    def test_safe_aliases_and_uncertain_aliases_are_separate(self) -> None:
        documents = (
            {
                "rows": (
                    {"ACTION": "  find   alt first  "},
                    {"ACTION": "Find Alt First"},
                    {"ACTION": "Find Alt Firs"},
                )
            },
        )
        registry = self.app.distillery_outcome_alias_registry(
            documents,
            self.profile,
        )
        action_entries = [
            item
            for item in registry["entries"]
            if item["field_name"] == "action"
        ]
        self.assertIn(
            self.app.distillery_action("Find Alt First"),
            {
                item["canonical_value"]
                for item in action_entries
            },
        )
        self.assertGreaterEqual(registry["review_required"], 1)

    def _synthetic_rows(self):
        app = self.app
        values = [
            ("2026-06-01", "Compass USA", "PRF", "OK"),
            ("2026-06-01", "Compass Canada", "PRF", "Review"),
            ("2026-06-02", "Compass USA", "PRF", "OK"),
            ("2026-06-02", "Compass Canada", "PRF", "Review"),
        ]
        rows = []
        for index, (group, business, request_type, outcome) in enumerate(
            values
        ):
            pair = app.DistilleryPair(
                pair_id=f"pair-{index}",
                source_group=group,
                before_index=index,
                after_index=index,
                before={"Business": business, "Type": request_type},
                after={},
                outputs={
                    "action": outcome,
                    "if_in_stock_action": "",
                    "audit_action": "",
                },
                method="synthetic",
                score=1.0,
            )
            features = {
                field: ""
                for field in self.profile["induction"]["governed_fields"]
            }
            features.update(
                {
                    "business": business,
                    "business_key": self.app.normalize_key(business),
                    "type": request_type,
                    "request_type_key": self.app.normalize_key(request_type),
                }
            )
            rows.append(
                app.DistilleryProjected(
                    pair=pair,
                    features=features,
                    label=tuple(sorted(pair.outputs.items())),
                )
            )
        return tuple(rows)

    def test_literal_filters_merge_across_dates_and_minimize(self) -> None:
        rows = self._synthetic_rows()
        mined = self.app.LiteralFilterMiner(self.profile).fit(rows)
        validation = self.app.distillery_validate(
            rows,
            mined["rules"],
            self.profile["induction"]["governed_fields"],
        )
        self.assertEqual(1.0, validation["accuracy"])
        self.assertEqual((), mined["gaps"])
        self.assertEqual((), mined["conflicts"])
        reusable = [
            rule for rule in mined["rules"] if rule.kind == "reusable"
        ]
        self.assertTrue(reusable)
        self.assertTrue(
            any(len(rule.source_groups) == 2 for rule in reusable)
        )
        for rule in reusable:
            for atom_index in range(len(rule.predicates)):
                reduced = (
                    rule.predicates[:atom_index]
                    + rule.predicates[atom_index + 1 :]
                )
                if not reduced:
                    continue
                coverage = [
                    row
                    for row in rows
                    if all(
                        self.app.distillery_evaluate_atom(
                            atom,
                            row.features,
                        )
                        for atom in reduced
                    )
                ]
                if coverage:
                    self.assertNotEqual(
                        {row.label for row in coverage},
                        {tuple(sorted(rule.outputs.items()))},
                    )

    def test_catalog_has_three_explicit_outputs_and_no_identity_predicates(
        self,
    ) -> None:
        rows = self._synthetic_rows()
        mined = self.app.LiteralFilterMiner(self.profile).fit(rows)
        catalog = self.app.literal_distillery_catalog(
            mined["rules"],
            self.profile,
            "synthetic-run",
            1.0,
            ["*"],
        )
        forbidden = {
            "__evidence_hash",
            "case",
            "case_number",
            "pair_id",
            "din",
        }
        for rule in catalog:
            variant = rule["variants"][0]
            action_types = {
                item["type"] for item in variant["action_json"]
            }
            self.assertEqual(
                {"set_action", "set_if_stock", "set_audit_action"},
                action_types,
            )
            self.assertTrue(
                all(
                    "value" in item
                    and item["explicit_final_state"] is True
                    for item in variant["action_json"]
                )
            )
            predicate_fields = {
                item["field"]
                for item in variant["predicate_json"]["all"]
            }
            self.assertTrue(predicate_fields.isdisjoint(forbidden))
            self.assertIn("logic_signature", rule["source"])

    def test_runtime_generalizes_across_case_din_and_sha_and_clears_blanks(
        self,
    ) -> None:
        app = self.app
        outputs = {
            "action": "OK",
            "if_in_stock_action": "",
            "audit_action": "",
        }
        rule = app.DistilleryRule(
            rule_id="LITERAL-SYNTHETIC",
            priority=1,
            predicates=(
                app.DistilleryAtom("business_key", "eq", "compass usa"),
                app.DistilleryAtom("request_type_key", "eq", "prf"),
            ),
            outputs=outputs,
            support=10,
            confidence=1.0,
            source_groups=("2026-06-01", "2026-06-02"),
            kind="reusable",
            evidence_ids=(),
        )
        catalog = app.literal_distillery_catalog(
            [rule],
            self.profile,
            "runtime-test",
            1.0,
            ["*"],
        )
        source = {
            "Business": "Compass USA",
            "Type": "PRF",
            "Case#": "TOTALLY-NEW-CASE",
            "DIN": "TOTALLY-NEW-DIN",
            "ACTION": "Old",
            "If In Stock: Action": "Old stock",
            "Audit Action": "Old audit",
        }
        row = app.create_workflow_row("runtime-test", source, 2)
        self.assertNotIn("__evidence_hash", app.context_for_row(row))
        executed, _, _ = app.execute_rows([row], catalog)
        self.assertEqual("OK", executed[0]["action"])
        self.assertEqual("", executed[0]["if_in_stock_action"])
        self.assertEqual("", executed[0]["audit_action"])

    def test_candidate_enablement_does_not_mutate_saved_rule_json(self) -> None:
        rules = [
            {
                "status": "ready",
                "variants": [
                    {
                        "enabled": False,
                        "status": "ready",
                        "is_executable": True,
                    }
                ],
            }
        ]
        before = deepcopy(rules)
        candidate = self.app.candidate_catalog_for_test(rules)
        self.assertEqual(before, rules)
        self.assertEqual("approved", candidate[0]["status"])
        self.assertTrue(candidate[0]["variants"][0]["enabled"])

    def test_catalog_version_comparison_is_read_only(self) -> None:
        app = self.app

        def catalog_for(action: str):
            rule = app.DistilleryRule(
                rule_id=f"LITERAL-{action}",
                priority=1,
                predicates=(
                    app.DistilleryAtom(
                        "business_key",
                        "eq",
                        "compass usa",
                    ),
                ),
                outputs={
                    "action": action,
                    "if_in_stock_action": "",
                    "audit_action": "",
                },
                support=2,
                confidence=1.0,
                source_groups=("2026-06-01", "2026-06-02"),
                kind="reusable",
                evidence_ids=(),
            )
            return app.literal_distillery_catalog(
                [rule],
                self.profile,
                f"run-{action}",
                1.0,
                ["*"],
            )

        class FakeStore:
            def __init__(self):
                self.active = catalog_for("OLD")
                self.candidate = catalog_for("NEW")
                self.write_calls = 0

            def get_catalog_version(self, version_id):
                return {
                    "id": version_id,
                    "workflow_id": "product_request",
                }

            def load_catalog_version_rules(self, _version_id):
                return deepcopy(self.candidate)

            def load_rules(self):
                return deepcopy(self.active)

            def load_reference_lists(self):
                return {}

            def upsert_rows(self, *_args, **_kwargs):
                self.write_calls += 1

        store = FakeStore()
        row = app.create_workflow_row(
            "candidate-test",
            {
                "Business": "Compass USA",
                "Type": "PRF",
                "Case#": "NEW-CASE",
            },
            2,
        )
        before = deepcopy(row)
        comparison = app.compare_catalog_version(
            store,
            "version-2",
            [row],
            source_label="synthetic",
        )
        self.assertEqual(before, row)
        self.assertEqual(0, store.write_calls)
        self.assertEqual(1, comparison["different_count"])
        self.assertEqual("OLD", comparison["records"][0]["Active ACTION"])
        self.assertEqual("NEW", comparison["records"][0]["Candidate ACTION"])

    def test_policy_boundary_rejects_identity_dates_notes_and_sampled_thresholds(
        self,
    ) -> None:
        app = self.app
        rejected = [
            app.DistilleryAtom("case_number", "eq", "WO-1"),
            app.DistilleryAtom("date_created", "date_after", "2026-01-01"),
            app.DistilleryAtom("reason_for_request", "contains", "menu"),
            app.DistilleryAtom("usage_num", "ge", 13.875),
            app.DistilleryAtom("description", "eq", "Exact product"),
        ]
        self.assertTrue(
            all(
                not app.distillery_policy_atom_allowed(atom, self.profile)
                for atom in rejected
            )
        )
        accepted = [
            app.DistilleryAtom("usage_num", "ge", 10),
            app.DistilleryAtom("usage_num", "lt", 15),
            app.DistilleryAtom("meets_criteria_num", "ge", 0.10),
            app.DistilleryAtom("business_key", "eq", "COMPASS USA"),
            app.DistilleryAtom("is_k12_apl", "is_true"),
        ]
        self.assertTrue(
            all(
                app.distillery_policy_atom_allowed(atom, self.profile)
                for atom in accepted
            )
        )

    def test_policy_grounded_miner_assigns_visible_stages(self) -> None:
        rows = self._synthetic_rows()
        policy_pack = {
            "clauses": [
                {
                    "rule_id": "R-ANCHOR",
                    "rule_group": "Business branch",
                    "decision_criteria": (
                        "Business and request type determine the outcome"
                    ),
                    "daily_action_file_columns": "Business; Type",
                    "set_action": "OK or Review",
                }
            ]
        }
        mined = self.app.PolicyGroundedFilterMiner(
            self.profile,
            policy_pack,
        ).fit(rows)
        validation = self.app.distillery_validate(
            rows,
            mined["rules"],
            self.profile["induction"]["governed_fields"],
        )
        self.assertEqual(1.0, validation["accuracy"])
        self.assertEqual((), mined["gaps"])
        self.assertTrue(mined["convergence"])
        self.assertTrue(
            all(rule.policy_stage > 0 for rule in mined["rules"])
        )
        self.assertTrue(
            all(
                rule.policy_stage_code
                and rule.amendment_status
                for rule in mined["rules"]
            )
        )

    def test_runtime_honors_preserve_set_and_clear_effects(self) -> None:
        app = self.app
        rule = app.DistilleryRule(
            rule_id="POLICY-EFFECTS",
            priority=1,
            predicates=(
                app.DistilleryAtom(
                    "business_key",
                    "eq",
                    "COMPASS USA",
                ),
            ),
            outputs={
                "action": "IGNORED FOR PRESERVE",
                "if_in_stock_action": "OK",
                "audit_action": "",
            },
            support=2,
            confidence=1.0,
            source_groups=("d1", "d2"),
            kind="reusable",
            evidence_ids=(),
            output_effects={
                "action": {
                    "effect": "preserve",
                    "input_field": "input_action",
                },
                "if_in_stock_action": {"effect": "set", "value": "OK"},
                "audit_action": {"effect": "clear", "value": ""},
            },
        )
        catalog = app.literal_distillery_catalog(
            [rule],
            self.profile,
            "effects-run",
            1.0,
            ["*"],
        )
        row = app.create_workflow_row(
            "effects",
            {
                "Business": "Compass USA",
                "Type": "PRF",
                "ACTION": "On MOG. Check Attribute.",
                "If In Stock: Action": "Review",
                "Audit Action": "SRF",
            },
            2,
        )
        executed, _, _ = app.execute_rows([row], catalog)
        self.assertEqual(
            "On MOG. Check Attribute.",
            executed[0]["action"],
        )
        self.assertEqual("OK", executed[0]["if_in_stock_action"])
        self.assertEqual("", executed[0]["audit_action"])

    def test_reference_enrichment_is_deterministic_and_versionable(self) -> None:
        app = self.app
        row = self._synthetic_rows()[0]
        pair = app.DistilleryPair(
            **{
                **row.pair.__dict__,
                "before": {
                    **row.pair.before,
                    "DIN": "123",
                    "Conversion DIN": "",
                },
            }
        )
        projected = app.DistilleryProjected(
            pair=pair,
            features={**row.features, "din": "123", "has_conversion": False},
            label=row.label,
        )
        reference_pack = {
            "datasets": {
                "conversion_mappings": {
                    "source_sha256": "abc",
                    "rows": [
                        {
                            "DIN": "123",
                            "Conversion DIN": "999",
                            "Conversion VA%": "10%",
                        }
                    ],
                }
            }
        }
        first, first_report = app.distillery_enrich_projected_rows(
            [projected],
            reference_pack,
        )
        second, second_report = app.distillery_enrich_projected_rows(
            [projected],
            reference_pack,
        )
        self.assertEqual(first[0].features, second[0].features)
        self.assertEqual(first_report, second_report)
        self.assertTrue(first[0].features["has_conversion"])
        self.assertEqual("999", first[0].features["conversion_din"])
        self.assertEqual(
            0.1,
            first[0].features["conversion_va_num"],
        )

    def test_policy_pack_promotes_titled_matrix_header_row(self) -> None:
        content = (
            "Daily Action File - Business Logic Matrix,Column 2,Column 3,"
            "Column 4,Column 5\n"
            "Rule ID,Rule Group,Business,Request Type (s),Decision Criteria\n"
            "R001,Pre-processing,All,All,Vendor exclusion\n"
            "R002,Business branch,Compass USA,PRF,Known branch\n"
        ).encode("utf-8")
        pack = self.app.distillery_policy_pack(
            [{"name": "logic_matrix.csv", "data": content}],
            self.profile,
        )
        self.assertTrue(pack["ready"])
        self.assertEqual(2, pack["clause_count"])
        self.assertEqual(
            ["R001", "R002"],
            [clause["rule_id"] for clause in pack["clauses"]],
        )

    def test_runtime_uses_versioned_typed_reference_features(self) -> None:
        app = self.app
        rule = app.DistilleryRule(
            rule_id="POLICY-REFERENCE",
            priority=1,
            predicates=(
                app.DistilleryAtom(
                    "ref_conversion_mappings",
                    "is_true",
                ),
            ),
            outputs={
                "action": "Use Right",
                "if_in_stock_action": "",
                "audit_action": "",
            },
            support=2,
            confidence=1.0,
            source_groups=("d1", "d2"),
            kind="reusable",
            evidence_ids=(),
        )
        catalog = app.literal_distillery_catalog(
            [rule],
            self.profile,
            "typed-reference",
            1.0,
            ["*"],
        )
        row = app.create_workflow_row(
            "typed-reference",
            {
                "Business": "Compass USA",
                "Type": "PRF",
                "DIN": "123",
            },
            2,
        )
        references = {
            "__typed_datasets__": {
                "conversion_mappings": [
                    {"DIN": "123", "Conversion DIN": "999"}
                ]
            }
        }
        executed, _, _ = app.execute_rows(
            [row],
            catalog,
            reference_lists=references,
        )
        self.assertEqual("Use Right", executed[0]["action"])


if __name__ == "__main__":
    unittest.main()

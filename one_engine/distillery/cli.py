from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import distill, save_run
from .profiles import load_profile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="one-engine-distillery",
        description=(
            "Mechanize rule discovery from paired BEFORE/AFTER evidence."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    distill_parser = subparsers.add_parser(
        "distill",
        help="Align paired sources, infer rules, validate, and emit a catalog.",
    )
    distill_parser.add_argument("--profile", required=True)
    distill_parser.add_argument("--before", type=Path, required=True)
    distill_parser.add_argument("--after", type=Path, required=True)
    distill_parser.add_argument("--output", type=Path, required=True)
    distill_parser.add_argument(
        "--skip-holdouts",
        action="store_true",
        help="Skip leave-one-source-group-out validation for a faster draft run.",
    )
    return parser


def run_distill(args: argparse.Namespace) -> int:
    profile = load_profile(args.profile)
    run = distill(
        profile=profile,
        before_path=args.before,
        after_path=args.after,
        run_holdouts=not args.skip_holdouts,
    )
    paths = save_run(run, profile, args.output)
    print(
        json.dumps(
            {
                "run_id": run.run_id,
                "profile_id": run.profile_id,
                "pairs": len(run.match_result.pairs),
                "unmatched": len(run.match_result.unmatched),
                "rules": len(run.rules),
                "general_rules": sum(
                    rule.kind == "general" for rule in run.rules
                ),
                "exception_rules": sum(
                    rule.kind == "exception" for rule in run.rules
                ),
                "corpus_accuracy": run.validation.accuracy,
                "holdout_accuracy": (
                    (run.diagnostics.get("holdout") or {}).get("mean_accuracy")
                ),
                "deployment_eligible": bool(
                    (run.diagnostics.get("deployment_gate") or {}).get("eligible")
                ),
                "outputs": {key: str(value) for key, value in paths.items()},
            },
            indent=2,
        )
    )
    return (
        0
        if bool(
            (run.diagnostics.get("deployment_gate") or {}).get("eligible")
        )
        else 2
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "distill":
        return run_distill(args)
    raise ValueError(f"Unknown command: {args.command}")

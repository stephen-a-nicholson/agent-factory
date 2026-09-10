"""CLI entry point for the bundle quality gates.

    uv run python -m gates.run --target dev

Runs `databricks bundle validate -t <target> -o json` (and, best-effort,
`databricks bundle plan -t <target> -o json`) and checks every rule in
gates.rules.ALL_RULES against the result. Exits non-zero if any rule
reports a fail-severity finding; warn findings are printed but do not
fail the run. --config/--plan accept a pre-captured JSON file instead of
invoking the CLI, for use in tests and fixtures.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from gates.loader import load_json
from gates.models import Finding, Severity
from gates.rules import ALL_RULES


def run_databricks_json(args: list[str]) -> dict[str, Any] | None:
    try:
        result = subprocess.run(args, capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"warning: {' '.join(args)} failed: {error}", file=sys.stderr)
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        print(f"warning: could not parse output of {' '.join(args)}: {error}", file=sys.stderr)
        return None


def run_gates(config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
    findings: list[Finding] = []
    for rule in ALL_RULES:
        findings.extend(rule.check(config, plan))
    return findings


def report_text(findings: list[Finding]) -> None:
    if not findings:
        print("all gates passed")
        return
    for finding in findings:
        location = f" [{finding.resource}]" if finding.resource else ""
        print(f"{finding.severity.value.upper():4} {finding.rule}: {finding.message}{location}")
    fails = sum(1 for f in findings if f.severity == Severity.FAIL)
    warns = sum(1 for f in findings if f.severity == Severity.WARN)
    print(f"\n{fails} fail, {warns} warn")


def report_json(findings: list[Finding]) -> None:
    print(
        json.dumps(
            [
                {
                    "rule": f.rule,
                    "severity": f.severity.value,
                    "message": f.message,
                    "resource": f.resource,
                }
                for f in findings
            ],
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="Bundle target, e.g. dev, test, prod")
    parser.add_argument(
        "--config", type=Path, default=None, help="Pre-captured `bundle validate -o json` file"
    )
    parser.add_argument(
        "--plan", type=Path, default=None, help="Pre-captured `bundle plan -o json` file"
    )
    parser.add_argument("--output", choices=["text", "json"], default="text")
    args = parser.parse_args()

    config = (
        load_json(args.config)
        if args.config
        else run_databricks_json(
            ["databricks", "bundle", "validate", "-t", args.target, "-o", "json"]
        )
    )
    if config is None:
        print("error: could not obtain bundle validate config", file=sys.stderr)
        sys.exit(2)

    plan = (
        load_json(args.plan)
        if args.plan
        else run_databricks_json(["databricks", "bundle", "plan", "-t", args.target, "-o", "json"])
    )

    findings = run_gates(config, plan)

    if args.output == "json":
        report_json(findings)
    else:
        report_text(findings)

    if any(f.severity == Severity.FAIL for f in findings):
        sys.exit(1)


if __name__ == "__main__":
    main()

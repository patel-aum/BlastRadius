#!/usr/bin/env python3
"""GitHub Action entry point — validates contract YAML and detects breaking changes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

# Add the contract_engine to path for shared models/validator
ENGINE_DIR = Path(__file__).resolve().parent.parent / "contract_engine"
sys.path.insert(0, str(ENGINE_DIR))
# Add the github-action dir itself for detect_breaking_changes
ACTION_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ACTION_DIR))

from validator import validate_against_schema, load_contract
from detect_breaking_changes import run_breaking_change_detection


def set_output(name: str, value: str) -> None:
    output_file = os.getenv("GITHUB_OUTPUT", "")
    if output_file:
        with open(output_file, "a") as f:
            if "\n" in value:
                import uuid as _uuid
                delim = f"ghadelimiter_{_uuid.uuid4()}"
                f.write(f"{name}<<{delim}\n{value}\n{delim}\n")
            else:
                f.write(f"{name}={value}\n")


def write_summary(md: str) -> None:
    summary_file = os.getenv("GITHUB_STEP_SUMMARY", "")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(md + "\n")


def main() -> None:
    contract_path = os.getenv("CONTRACT_PATH", "")
    schema_path = os.getenv("SCHEMA_PATH", "schemas/contract-schema.json")
    base_ref = os.getenv("BASE_REF", "origin/main")
    fail_on_breaking = os.getenv("FAIL_ON_BREAKING", "true").lower() == "true"

    if not contract_path:
        print("::error::CONTRACT_PATH is required")
        sys.exit(1)

    report_lines: list[str] = ["## Data Contract Validation Report\n"]
    has_errors = False

    # ── Stage 1: Meta-schema validation ─────────────────────────
    report_lines.append("### 1. Meta-Schema Validation\n")
    errors = validate_against_schema(contract_path)
    if errors:
        has_errors = True
        report_lines.append("**FAIL** — Schema validation errors:\n")
        for e in errors:
            report_lines.append(f"- {e}")
    else:
        report_lines.append("**PASS** — Contract conforms to ODCS v3.1.0\n")

    # ── Stage 2: Contract parsing ───────────────────────────────
    report_lines.append("\n### 2. Contract Parsing\n")
    try:
        contract = load_contract(contract_path)
        report_lines.append(f"**PASS** — `{contract.data_product}` v{contract.version} ({contract.domain})\n")
    except Exception as exc:
        has_errors = True
        report_lines.append(f"**FAIL** — Could not parse contract: {exc}\n")
        contract = None

    # ── Stage 3: Breaking changes ───────────────────────────────
    report_lines.append("\n### 3. Breaking Change Detection\n")
    breaking = run_breaking_change_detection(contract_path, base_ref)
    if breaking is None:
        report_lines.append("**SKIP** — No previous version found on base branch\n")
    elif len(breaking) == 0:
        report_lines.append("**PASS** — No breaking changes detected\n")
    else:
        if fail_on_breaking:
            has_errors = True
        report_lines.append(f"**{'FAIL' if fail_on_breaking else 'WARN'}** — {len(breaking)} breaking change(s):\n")
        report_lines.append("| Type | Severity | Details |")
        report_lines.append("|------|----------|---------|")
        for b in breaking:
            report_lines.append(f"| {b['type']} | {b['severity']} | {b['message']} |")

    # ── Stage 4: Business rules ─────────────────────────────────
    report_lines.append("\n### 4. Business Rule Checks\n")
    if contract:
        biz_issues: list[str] = []
        if not contract.team:
            biz_issues.append("Missing `team` definition")
        if not contract.sla_properties:
            biz_issues.append("Missing `slaProperties`")
        if not contract.quality and not any(s.quality for s in contract.schema_objects):
            biz_issues.append("No quality rules defined at any level")

        if biz_issues:
            report_lines.append("**WARN** — Business rule issues:\n")
            for issue in biz_issues:
                report_lines.append(f"- {issue}")
        else:
            report_lines.append("**PASS** — All business rules satisfied\n")
    else:
        report_lines.append("**SKIP** — Contract could not be parsed\n")

    # ── Summary ─────────────────────────────────────────────────
    result = "FAIL" if has_errors else "PASS"
    report_lines.insert(1, f"\n**Overall Result: {result}**\n")

    report_md = "\n".join(report_lines)
    print(report_md)

    set_output("result", result)
    set_output("report", report_md)
    write_summary(report_md)

    if has_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

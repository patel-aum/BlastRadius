"""Contract validation logic — shared between CI and MCP server."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from models import DataContract, QualityRule

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "contract-schema.json"
if not SCHEMA_PATH.exists():
    SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "contract-schema.json"
if not SCHEMA_PATH.exists():
    SCHEMA_PATH = Path("/schemas/contract-schema.json")


def load_contract(path: str | Path) -> DataContract:
    """Parse a YAML file into a DataContract model."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return DataContract.model_validate(raw)


def load_all_contracts(directory: str | Path) -> list[tuple[Path, DataContract]]:
    """Load every *.yaml / *.yml contract from a directory."""
    contracts: list[tuple[Path, DataContract]] = []
    d = Path(directory)
    for p in sorted(d.glob("*.yaml")) + sorted(d.glob("*.yml")):
        try:
            contracts.append((p, load_contract(p)))
        except Exception as exc:
            logger.warning("Skipping %s: %s", p.name, exc)
    return contracts


def file_hash(path: str | Path) -> str:
    """SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Meta-schema validation ──────────────────────────────────────


def validate_against_schema(contract_path: str | Path) -> list[str]:
    """Validate a YAML contract against the ODCS JSON meta-schema. Returns error messages."""
    errors: list[str] = []
    schema_file = SCHEMA_PATH
    if not schema_file.exists():
        return ["Meta-schema file not found — skipping structural validation"]

    with open(schema_file) as f:
        meta_schema = json.load(f)
    with open(contract_path) as f:
        raw = yaml.safe_load(f)

    validator = jsonschema.Draft7Validator(meta_schema)
    for err in validator.iter_errors(raw):
        path_str = ".".join(str(p) for p in err.absolute_path) or "(root)"
        errors.append(f"[{path_str}] {err.message}")
    return errors


# ── Quality rule evaluation against profiler data ───────────────


def _safe_numeric(val: Any) -> float | None:
    """Safely coerce a value to float for comparison."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _evaluate_rule(rule: QualityRule, actual_value: Any) -> dict:
    """Check a single quality rule against an actual metric value."""
    result: dict[str, Any] = {
        "metric": rule.metric,
        "severity": rule.severity.value,
        "expected": {},
        "actual": actual_value,
        "passed": True,
        "message": "",
    }

    if actual_value is None:
        result["passed"] = False
        result["message"] = "No profiler data available for this metric"
        return result

    numeric_actual = _safe_numeric(actual_value)

    checks: list[tuple[str, Any, bool]] = []
    if rule.must_be is not None:
        checks.append(("mustBe", rule.must_be, actual_value == rule.must_be or numeric_actual == _safe_numeric(rule.must_be)))
    if rule.must_be_greater_than is not None and numeric_actual is not None:
        checks.append(("mustBeGreaterThan", rule.must_be_greater_than, numeric_actual > rule.must_be_greater_than))
    if rule.must_be_less_than is not None and numeric_actual is not None:
        checks.append(("mustBeLessThan", rule.must_be_less_than, numeric_actual < rule.must_be_less_than))
    if rule.must_be_greater_than_or_equal_to is not None and numeric_actual is not None:
        checks.append(("mustBeGte", rule.must_be_greater_than_or_equal_to, numeric_actual >= rule.must_be_greater_than_or_equal_to))
    if rule.must_be_less_than_or_equal_to is not None and numeric_actual is not None:
        checks.append(("mustBeLte", rule.must_be_less_than_or_equal_to, numeric_actual <= rule.must_be_less_than_or_equal_to))
    if rule.must_not_be is not None:
        checks.append(("mustNotBe", rule.must_not_be, actual_value != rule.must_not_be))

    for label, expected, passed in checks:
        result["expected"][label] = expected
        if not passed:
            result["passed"] = False
            result["message"] = f"{rule.metric}: expected {label}={expected}, got {actual_value}"
            break

    if result["passed"]:
        result["message"] = "OK"
    return result


def evaluate_quality_rules(
    contract: DataContract,
    profile_data: dict | None,
    test_results: list[dict] | None = None,
) -> list[dict]:
    """Evaluate all quality rules in a contract against OM profiler data."""
    results: list[dict] = []
    profile = profile_data or {}
    all_rules = contract.get_all_quality_rules()

    metric_to_profile_key = {
        "rowCount": "rowCount",
        "nullValues": "nullCount",
        "duplicateValues": "duplicateCount",
        "invalidValues": "invalidCount",
        "freshness": "freshness",
    }

    for col_name, rule in all_rules:
        actual = None
        profile_key = metric_to_profile_key.get(rule.metric, rule.metric)

        if col_name is None:
            actual = profile.get(profile_key)
        else:
            col_profiles = profile.get("columnProfile", [])
            if isinstance(col_profiles, list):
                for cp in col_profiles:
                    if cp.get("name") == col_name:
                        actual = cp.get(profile_key)
                        break

        r = _evaluate_rule(rule, actual)
        r["column"] = col_name
        results.append(r)

    return results


# ── Breaking-change detection between two contract versions ─────


def detect_breaking_changes(old_path: str | Path, new_path: str | Path) -> list[dict]:
    """Compare two contract YAML files and return breaking changes."""
    old = load_contract(old_path)
    new = load_contract(new_path)
    issues: list[dict] = []

    old_tables = {s.name: s for s in old.schema_objects}
    new_tables = {s.name: s for s in new.schema_objects}

    for tname, old_schema in old_tables.items():
        if tname not in new_tables:
            issues.append({
                "type": "TABLE_REMOVED",
                "severity": "error",
                "message": f"Table '{tname}' was removed",
            })
            continue

        new_schema = new_tables[tname]
        old_cols = {c.name: c for c in old_schema.properties}
        new_cols = {c.name: c for c in new_schema.properties}

        for cname, old_col in old_cols.items():
            if cname not in new_cols:
                issues.append({
                    "type": "COLUMN_REMOVED",
                    "severity": "error",
                    "table": tname,
                    "message": f"Column '{cname}' was removed from '{tname}'",
                })
                continue

            new_col = new_cols[cname]

            if old_col.logical_type != new_col.logical_type:
                issues.append({
                    "type": "TYPE_CHANGED",
                    "severity": "error",
                    "table": tname,
                    "column": cname,
                    "message": f"Column '{cname}' type changed: {old_col.logical_type.value} -> {new_col.logical_type.value}",
                })

            if not old_col.required and new_col.required:
                issues.append({
                    "type": "MADE_REQUIRED",
                    "severity": "error",
                    "table": tname,
                    "column": cname,
                    "message": f"Column '{cname}' changed from optional to required",
                })

    return issues

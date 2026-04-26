"""MCP Server — Data Contract Governance tools for AI agents."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Annotated

from pydantic import Field

# In Docker, PYTHONPATH=/contract_engine is set via docker-compose.
# For local dev, fall back to the sibling directory.
_engine_dir = os.getenv("PYTHONPATH", "")
if not _engine_dir:
    _engine_dir = str(Path(__file__).resolve().parent.parent / "contract_engine")
if _engine_dir not in sys.path:
    sys.path.insert(0, _engine_dir)

from mcp.server.fastmcp import FastMCP
from models import DataContract
from om_client import OpenMetadataClient
from validator import load_all_contracts, evaluate_quality_rules, validate_against_schema
from drift import detect_schema_drift

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("mcp-server")

CONTRACTS_DIR = os.getenv("CONTRACTS_DIR", "/contracts")
SERVICE_NAME = "demo_postgres"
MCP_HOST = "0.0.0.0"
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))

mcp = FastMCP(
    "DataContractGovernance",
    instructions=(
        "This MCP server provides tools for querying and validating data contracts "
        "against OpenMetadata. Use validate_contract to check compliance, list_violations "
        "to find all broken contracts, get_contract_status for a health summary, and "
        "detect_schema_drift to compare declared vs actual schemas."
    ),
    host=MCP_HOST,
    port=MCP_PORT,
)


def _get_client() -> OpenMetadataClient:
    return OpenMetadataClient()


def _load_contracts() -> list[tuple[Path, DataContract]]:
    return load_all_contracts(CONTRACTS_DIR)


def _resolve_table_fqn(contract: DataContract) -> str | None:
    if not contract.server or not contract.schema_objects:
        return None
    return (
        f"{SERVICE_NAME}.{contract.server.database}"
        f".{contract.server.schema_name}.{contract.schema_objects[0].name}"
    )


@mcp.tool()
def validate_contract(
    contract_name: Annotated[str, Field(description="Name of the data product or contract file (e.g. 'seller-transactions' or 'seller-transactions.yaml')")],
) -> str:
    """Validate a specific data contract against OpenMetadata.

    Checks: meta-schema compliance, quality rule evaluation, and SLA status.
    Returns a structured JSON compliance report.
    """
    contracts = _load_contracts()
    target = None
    target_path = None

    clean = contract_name.replace(".yaml", "").replace(".yml", "")
    for path, c in contracts:
        if c.data_product == clean or path.stem == clean:
            target = c
            target_path = path
            break

    if not target or not target_path:
        return json.dumps({"error": f"Contract '{contract_name}' not found. Available: {[c.data_product for _, c in contracts]}"})

    report: dict = {
        "contract": target.data_product,
        "version": target.version,
        "status": target.status.value,
        "domain": target.domain,
        "checks": {},
    }

    schema_errors = validate_against_schema(target_path)
    report["checks"]["meta_schema"] = {
        "passed": len(schema_errors) == 0,
        "errors": schema_errors,
    }

    table_fqn = _resolve_table_fqn(target)
    if table_fqn:
        client = _get_client()
        profile = client.get_table_profile(table_fqn)
        quality_results = evaluate_quality_rules(target, profile)
        passed = sum(1 for r in quality_results if r["passed"])
        total = len(quality_results)
        report["checks"]["quality"] = {
            "passed": passed == total,
            "score": f"{passed}/{total}",
            "details": quality_results,
        }

        report["checks"]["sla"] = {
            "properties": [
                {"property": s.property, "value": s.value, "unit": s.unit}
                for s in target.sla_properties
            ],
        }
    else:
        report["checks"]["quality"] = {"passed": False, "error": "No table FQN resolvable"}

    all_passed = all(
        c.get("passed", False) for c in report["checks"].values() if isinstance(c, dict) and "passed" in c
    )
    report["overall_compliance"] = "COMPLIANT" if all_passed else "NON_COMPLIANT"

    return json.dumps(report, indent=2, default=str)


@mcp.tool()
def list_violations() -> str:
    """List all data contracts that are currently violated.

    Scans every contract YAML, checks against OpenMetadata profiler and quality data,
    and returns only those with failing checks.
    """
    contracts = _load_contracts()
    client = _get_client()
    violations: list[dict] = []

    for path, contract in contracts:
        table_fqn = _resolve_table_fqn(contract)
        if not table_fqn:
            continue

        profile = client.get_table_profile(table_fqn)
        quality_results = evaluate_quality_rules(contract, profile)
        failed = [r for r in quality_results if not r["passed"]]

        om_table = client.get_table_by_fqn(table_fqn)
        drift_report = detect_schema_drift(contract, om_table)

        if failed or drift_report["has_drift"]:
            violations.append({
                "contract": contract.data_product,
                "version": contract.version,
                "domain": contract.domain,
                "table_fqn": table_fqn,
                "quality_violations": failed,
                "schema_drift": drift_report["drifts"] if drift_report["has_drift"] else [],
            })

    summary = {
        "total_contracts": len(contracts),
        "violated": len(violations),
        "compliant": len(contracts) - len(violations),
        "violations": violations,
    }
    return json.dumps(summary, indent=2, default=str)


@mcp.tool()
def get_contract_status(
    contract_name: Annotated[str, Field(description="Name of the data product (e.g. 'seller-transactions')")],
) -> str:
    """Get the overall health status of a specific data contract.

    Returns: schema drift status, quality test results, freshness check, SLA compliance.
    """
    contracts = _load_contracts()
    clean = contract_name.replace(".yaml", "").replace(".yml", "")
    target = None
    for _, c in contracts:
        if c.data_product == clean:
            target = c
            break

    if not target:
        return json.dumps({"error": f"Contract '{contract_name}' not found"})

    table_fqn = _resolve_table_fqn(target)
    client = _get_client()

    status: dict = {
        "contract": target.data_product,
        "version": target.version,
        "domain": target.domain,
        "status": target.status.value,
        "team": target.team.name if target.team else None,
        "table_fqn": table_fqn,
        "health": {},
    }

    if table_fqn:
        om_table = client.get_table_by_fqn(table_fqn)
        drift_report = detect_schema_drift(target, om_table)
        status["health"]["schema_drift"] = {
            "has_drift": drift_report["has_drift"],
            "summary": drift_report["summary"],
            "details": drift_report["drifts"][:5],
        }

        profile = client.get_table_profile(table_fqn)
        quality_results = evaluate_quality_rules(target, profile)
        passed = sum(1 for r in quality_results if r["passed"])
        total = len(quality_results)
        status["health"]["quality"] = {
            "score": f"{passed}/{total}",
            "pass_rate": round(passed / total * 100, 1) if total > 0 else 0,
            "failed_rules": [r for r in quality_results if not r["passed"]],
        }

        status["health"]["sla"] = [
            {"property": s.property, "value": s.value, "unit": s.unit}
            for s in target.sla_properties
        ]

        all_ok = not drift_report["has_drift"] and passed == total
        status["overall_health"] = "HEALTHY" if all_ok else "DEGRADED"
    else:
        status["overall_health"] = "UNKNOWN"
        status["health"]["error"] = "Cannot resolve table FQN"

    return json.dumps(status, indent=2, default=str)


@mcp.tool()
def detect_drift(
    contract_name: Annotated[str, Field(description="Name of the data product (e.g. 'seller-transactions')")],
) -> str:
    """Detect schema drift between a contract's declared schema and the actual table in OpenMetadata.

    Identifies: added columns not in contract, removed columns, type mismatches, nullability changes.
    This is the key tool for catching producer-side schema changes that break consumer contracts.
    """
    contracts = _load_contracts()
    clean = contract_name.replace(".yaml", "").replace(".yml", "")
    target = None
    for _, c in contracts:
        if c.data_product == clean:
            target = c
            break

    if not target:
        return json.dumps({"error": f"Contract '{contract_name}' not found"})

    table_fqn = _resolve_table_fqn(target)
    if not table_fqn:
        return json.dumps({"error": "Cannot resolve table FQN from contract"})

    client = _get_client()
    om_table = client.get_table_by_fqn(table_fqn)
    drift_report = detect_schema_drift(target, om_table)

    return json.dumps(drift_report, indent=2, default=str)


if __name__ == "__main__":
    logger.info("Starting MCP server on %s:%d", MCP_HOST, MCP_PORT)
    mcp.run(transport="streamable-http")

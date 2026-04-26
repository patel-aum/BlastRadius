#!/usr/bin/env python3
"""Demo script — creates a contract violation by modifying a table's schema in OpenMetadata."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

os.environ.setdefault("OM_SERVER_URL", "http://localhost:8585/api")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "contract_engine"))

from om_client import OpenMetadataClient
from validator import load_contract
from drift import detect_schema_drift

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("demo-violation")

SERVICE_NAME = "demo_postgres"


def simulate_schema_drift(client: OpenMetadataClient) -> None:
    """Add an undeclared column and remove a declared one from seller_transactions.

    This simulates a producer team making schema changes without updating the contract.
    """
    table_fqn = f"{SERVICE_NAME}.analytics_db.public.seller_transactions"
    table = client.get_table_by_fqn(table_fqn, fields="columns")

    if not table:
        logger.error("Table %s not found — run seed/setup.py first", table_fqn)
        return

    logger.info("Current columns: %s", [c["name"] for c in table.get("columns", [])])

    existing_cols = table.get("columns", [])
    modified_cols = [c for c in existing_cols if c["name"] != "currency"]
    modified_cols.append({
        "name": "payment_method",
        "dataType": "VARCHAR",
        "description": "Payment method (not in contract!)",
        "constraint": "NULL",
    })
    modified_cols.append({
        "name": "discount_pct",
        "dataType": "FLOAT",
        "description": "Discount percentage (not in contract!)",
        "constraint": "NULL",
    })

    schema_fqn = f"{SERVICE_NAME}.analytics_db.public"
    try:
        client.create_table(schema_fqn, "seller_transactions", modified_cols)
        logger.info("Modified seller_transactions: removed 'currency', added 'payment_method' and 'discount_pct'")
    except Exception as exc:
        logger.error("Failed to modify table: %s", exc)
        return

    contract_path = Path(__file__).resolve().parent.parent / "contracts" / "seller-transactions.yaml"
    contract = load_contract(contract_path)
    updated_table = client.get_table_by_fqn(table_fqn, fields="columns")
    drift_report = detect_schema_drift(contract, updated_table)

    logger.info("=" * 60)
    logger.info("DRIFT REPORT:")
    logger.info("=" * 60)
    print(json.dumps(drift_report, indent=2))
    logger.info("=" * 60)

    if drift_report["has_drift"]:
        logger.warning(
            "Schema drift detected! %d issue(s) found. "
            "This is what happens when a producer changes the schema without updating the contract.",
            len(drift_report["drifts"]),
        )
    else:
        logger.info("No drift detected (unexpected in demo mode)")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Demo: Simulating Contract Violation")
    logger.info("=" * 60)

    client = OpenMetadataClient()
    if not client.health_check():
        logger.error("OpenMetadata is not reachable")
        sys.exit(1)

    simulate_schema_drift(client)

    logger.info("\nNext steps:")
    logger.info("  1. Check the Dashboard at http://localhost:8501 — 'Violations' page")
    logger.info("  2. Use the MCP tool: detect_drift('seller-transactions')")
    logger.info("  3. Use the MCP tool: list_violations()")


if __name__ == "__main__":
    main()

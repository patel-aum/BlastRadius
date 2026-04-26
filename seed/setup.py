#!/usr/bin/env python3
"""Seed script — bootstraps OpenMetadata with sample entities and syncs contracts."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

# Set CONTRACTS_DIR before importing sync (which reads it at call time)
_contracts_path = str(Path(__file__).resolve().parent.parent / "contracts")
os.environ.setdefault("CONTRACTS_DIR", _contracts_path)
os.environ.setdefault("OM_SERVER_URL", "http://localhost:8585/api")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "contract_engine"))

from om_client import OpenMetadataClient
from sync import main as run_sync, wait_for_om, ensure_classification

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("seed")

SERVICE_NAME = "demo_postgres"


def create_sample_service(client: OpenMetadataClient) -> None:
    """Create a demo database service with database, schema, and tables."""
    logger.info("Creating demo database service: %s", SERVICE_NAME)

    try:
        client.create_database_service(SERVICE_NAME, "Postgres")
    except Exception as exc:
        logger.info("Service may already exist: %s", exc)

    try:
        client.create_database(SERVICE_NAME, "analytics_db")
    except Exception as exc:
        logger.info("Database may already exist: %s", exc)

    try:
        client.create_database_schema(f"{SERVICE_NAME}.analytics_db", "public")
    except Exception as exc:
        logger.info("Schema may already exist: %s", exc)

    tables = {
        "seller_transactions": [
            {"name": "txn_id", "dataType": "VARCHAR", "description": "Unique transaction ID", "constraint": "NOT_NULL"},
            {"name": "seller_id", "dataType": "VARCHAR", "description": "Seller reference", "constraint": "NOT_NULL"},
            {"name": "txn_ref_dt", "dataType": "DATE", "description": "Transaction date", "constraint": "NOT_NULL"},
            {"name": "amount", "dataType": "DOUBLE", "description": "Amount in USD", "constraint": "NOT_NULL"},
            {"name": "currency", "dataType": "VARCHAR", "description": "ISO currency code", "constraint": "NOT_NULL"},
            {"name": "status", "dataType": "VARCHAR", "description": "Transaction status", "constraint": "NOT_NULL"},
            {"name": "created_at", "dataType": "TIMESTAMP", "description": "Created timestamp", "constraint": "NOT_NULL"},
        ],
        "buyer_orders": [
            {"name": "order_id", "dataType": "VARCHAR", "description": "Unique order ID", "constraint": "NOT_NULL"},
            {"name": "buyer_id", "dataType": "VARCHAR", "description": "Buyer reference", "constraint": "NOT_NULL"},
            {"name": "order_date", "dataType": "DATE", "description": "Order date", "constraint": "NOT_NULL"},
            {"name": "total_amount", "dataType": "DOUBLE", "description": "Total amount", "constraint": "NOT_NULL"},
            {"name": "item_count", "dataType": "INT", "description": "Number of items", "constraint": "NOT_NULL"},
            {"name": "shipping_status", "dataType": "VARCHAR", "description": "Shipping status", "constraint": "NOT_NULL"},
            {"name": "region", "dataType": "VARCHAR", "description": "Buyer region", "constraint": "NULL"},
            {"name": "updated_at", "dataType": "TIMESTAMP", "description": "Last update", "constraint": "NOT_NULL"},
        ],
        "product_catalog": [
            {"name": "product_id", "dataType": "VARCHAR", "description": "Product SKU", "constraint": "NOT_NULL"},
            {"name": "product_name", "dataType": "VARCHAR", "description": "Product name", "constraint": "NOT_NULL"},
            {"name": "category", "dataType": "VARCHAR", "description": "Category path", "constraint": "NOT_NULL"},
            {"name": "price", "dataType": "DOUBLE", "description": "Listed price USD", "constraint": "NOT_NULL"},
            {"name": "is_active", "dataType": "BOOLEAN", "description": "Active listing flag", "constraint": "NOT_NULL"},
            {"name": "created_at", "dataType": "TIMESTAMP", "description": "Created timestamp", "constraint": "NOT_NULL"},
            {"name": "updated_at", "dataType": "TIMESTAMP", "description": "Last update", "constraint": "NOT_NULL"},
        ],
    }

    schema_fqn = f"{SERVICE_NAME}.analytics_db.public"
    for table_name, columns in tables.items():
        try:
            client.create_table(schema_fqn, table_name, columns)
            logger.info("Created table: %s.%s", schema_fqn, table_name)
        except Exception as exc:
            logger.info("Table %s may already exist: %s", table_name, exc)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Seed Script — Bootstrap OpenMetadata")
    logger.info("=" * 60)

    client = OpenMetadataClient()
    wait_for_om(client)

    create_sample_service(client)
    ensure_classification(client)

    logger.info("Running contract sync...")
    run_sync()

    logger.info("=" * 60)
    logger.info("Seed complete!")
    logger.info("  OpenMetadata UI: http://localhost:8585")
    logger.info("  Dashboard:       http://localhost:8501")
    logger.info("  MCP Server:      http://localhost:8000/mcp")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

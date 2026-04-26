"""Contract Sync Engine — reads YAML contracts and syncs to OpenMetadata."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from models import DataContract
from om_client import OpenMetadataClient
from validator import load_all_contracts, file_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("contract-sync")

CONTRACTS_DIR = os.getenv("CONTRACTS_DIR", "/contracts")
CLASSIFICATION_NAME = "DataContract"

LOGICAL_TYPE_TO_OM = {
    "string": "VARCHAR",
    "integer": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "binary": "BINARY",
    "array": "ARRAY",
    "object": "JSON",
    "uuid": "VARCHAR",
}

QUALITY_METRIC_TO_OM_TEST = {
    "rowCount": "tableRowCountToBeBetween",
    "nullValues": "columnValuesToBeNotNull",
    "duplicateValues": "columnValuesToBeUnique",
    "freshness": "tableCustomSQLQuery",
}


def wait_for_om(client: OpenMetadataClient, retries: int = 30, delay: int = 10) -> None:
    for attempt in range(1, retries + 1):
        if client.health_check():
            logger.info("OpenMetadata API is healthy (attempt %d)", attempt)
            return
        logger.info("Waiting for OpenMetadata API... attempt %d/%d", attempt, retries)
        time.sleep(delay)
    raise RuntimeError("OpenMetadata API did not become healthy in time")


def ensure_classification(client: OpenMetadataClient) -> None:
    """Create the DataContract classification and common tags."""
    logger.info("Ensuring classification '%s' exists", CLASSIFICATION_NAME)
    client.create_or_update_classification(
        CLASSIFICATION_NAME,
        description="Tags applied by the Data Contract Governance Engine",
    )

    for tag in ["Active", "Deprecated", "Retired", "Draft", "Compliant", "Violated"]:
        client.create_or_update_tag(
            CLASSIFICATION_NAME,
            tag,
            description=f"Contract status: {tag}",
        )


def ensure_table_exists(
    client: OpenMetadataClient,
    contract: DataContract,
    service_name: str = "demo_postgres",
) -> str | None:
    """Ensure the OM table entity exists; create stub if not. Returns table FQN."""
    if not contract.server or not contract.schema_objects:
        return None

    db_name = contract.server.database
    schema_name = contract.server.schema_name
    table_name = contract.schema_objects[0].name

    fqn = f"{service_name}.{db_name}.{schema_name}.{table_name}"

    existing = client.get_table_by_fqn(fqn)
    if existing:
        logger.info("Table %s already exists", fqn)
        return fqn

    logger.info("Creating table stub: %s", fqn)
    try:
        client.create_database_service(service_name, contract.server.type.capitalize() or "Postgres")
    except Exception:
        pass

    try:
        client.create_database(service_name, db_name)
    except Exception:
        pass

    try:
        client.create_database_schema(f"{service_name}.{db_name}", schema_name)
    except Exception:
        pass

    columns = []
    for col in contract.schema_objects[0].properties:
        columns.append({
            "name": col.name,
            "dataType": LOGICAL_TYPE_TO_OM.get(col.logical_type.value, "VARCHAR"),
            "description": col.description or "",
            "constraint": "NOT_NULL" if col.required else "NULL",
        })

    try:
        client.create_table(
            f"{service_name}.{db_name}.{schema_name}",
            table_name,
            columns,
        )
    except Exception as exc:
        logger.warning("Could not create table %s: %s", fqn, exc)

    return fqn


def sync_tags(client: OpenMetadataClient, contract: DataContract, table_fqn: str) -> None:
    """Apply contract-derived tags to the table."""
    table = client.get_table_by_fqn(table_fqn, fields="tags")
    if not table:
        return

    table_id = table["id"]
    status_tag = f"{CLASSIFICATION_NAME}.{contract.status.value.capitalize()}"

    existing_tags = {t.get("tagFQN") for t in table.get("tags", [])}

    for tag_fqn in [status_tag]:
        if tag_fqn not in existing_tags:
            try:
                client.add_tag_to_table(table_id, tag_fqn)
                logger.info("Tagged %s with %s", table_fqn, tag_fqn)
            except Exception as exc:
                logger.warning("Failed to tag %s: %s", table_fqn, exc)

    for tag_name in contract.tags:
        safe_name = tag_name.replace(" ", "_").replace("-", "_")
        try:
            client.create_or_update_tag(
                CLASSIFICATION_NAME, safe_name, description=f"Contract tag: {tag_name}"
            )
            tag_full = f"{CLASSIFICATION_NAME}.{safe_name}"
            if tag_full not in existing_tags:
                client.add_tag_to_table(table_id, tag_full)
        except Exception as exc:
            logger.debug("Tag sync issue for %s: %s", tag_name, exc)


def sync_policy(client: OpenMetadataClient, contract: DataContract) -> None:
    """Create a governance policy from the contract's roles."""
    if not contract.roles:
        return

    policy_name = f"contract-{contract.data_product}-policy"
    rules = []
    for role in contract.roles:
        operations = ["ViewAll"] if role.access == "read" else ["ViewAll", "EditAll"]
        rules.append({
            "name": f"{role.role}-rule",
            "description": f"Auto-generated from contract {contract.data_product}",
            "resources": ["table"],
            "operations": operations,
            "effect": "allow",
        })

    try:
        client.create_or_update_policy(policy_name, rules, description=f"Policy for contract: {contract.data_product} v{contract.version}")
        logger.info("Synced policy: %s", policy_name)
    except Exception as exc:
        logger.warning("Failed to sync policy %s: %s", policy_name, exc)


def sync_quality_tests(
    client: OpenMetadataClient, contract: DataContract, table_fqn: str
) -> None:
    """Create OM test cases from contract quality rules."""
    all_rules = contract.get_all_quality_rules()
    if not all_rules:
        return

    suite_name = f"{table_fqn}.testSuite"

    for col_name, rule in all_rules:
        om_test_type = QUALITY_METRIC_TO_OM_TEST.get(rule.metric)
        if not om_test_type:
            continue

        test_name = f"contract_{contract.data_product}_{rule.metric}"
        if col_name:
            test_name = f"{test_name}_{col_name}"
        test_name = test_name.replace("-", "_").replace(".", "_")

        params = {}
        if rule.must_be is not None:
            params["value"] = str(rule.must_be)
        if rule.must_be_greater_than is not None:
            params["minValue"] = str(rule.must_be_greater_than)
        if rule.must_be_less_than is not None:
            params["maxValue"] = str(rule.must_be_less_than)

        try:
            client.create_test_case(
                test_suite_fqn=suite_name,
                table_fqn=table_fqn,
                name=test_name,
                test_type=om_test_type,
                params=params if params else None,
                column_name=col_name,
            )
            logger.info("Created test case: %s", test_name)
        except Exception as exc:
            logger.debug("Test case sync issue for %s: %s", test_name, exc)


def sync_contract(client: OpenMetadataClient, contract: DataContract) -> dict:
    """Full sync of a single contract to OpenMetadata. Returns a status dict."""
    result = {"contract": contract.data_product, "version": contract.version, "steps": {}}

    table_fqn = ensure_table_exists(client, contract)
    result["table_fqn"] = table_fqn

    if not table_fqn:
        result["steps"]["table"] = "skipped — no server/schema config"
        return result

    result["steps"]["table"] = "ok"

    try:
        sync_tags(client, contract, table_fqn)
        result["steps"]["tags"] = "ok"
    except Exception as exc:
        result["steps"]["tags"] = f"error: {exc}"

    try:
        sync_policy(client, contract)
        result["steps"]["policy"] = "ok"
    except Exception as exc:
        result["steps"]["policy"] = f"error: {exc}"

    try:
        sync_quality_tests(client, contract, table_fqn)
        result["steps"]["quality_tests"] = "ok"
    except Exception as exc:
        result["steps"]["quality_tests"] = f"error: {exc}"

    return result


def main() -> None:
    contracts_dir = os.getenv("CONTRACTS_DIR", "/contracts")
    logger.info("=" * 60)
    logger.info("Data Contract Sync Engine starting")
    logger.info("Contracts directory: %s", contracts_dir)
    logger.info("=" * 60)

    client = OpenMetadataClient()
    wait_for_om(client)
    ensure_classification(client)

    contracts = load_all_contracts(contracts_dir)
    logger.info("Found %d contract(s)", len(contracts))

    results = []
    for path, contract in contracts:
        logger.info("Syncing contract: %s (v%s) from %s", contract.data_product, contract.version, path.name)
        r = sync_contract(client, contract)
        results.append(r)
        logger.info("  Result: %s", json.dumps(r, indent=2))

    logger.info("=" * 60)
    logger.info("Sync complete. %d contract(s) processed.", len(results))
    for r in results:
        status_emoji = "OK" if all(v == "ok" for v in r["steps"].values()) else "WARN"
        logger.info("  [%s] %s v%s -> %s", status_emoji, r["contract"], r["version"], r.get("table_fqn", "N/A"))
    logger.info("=" * 60)

    shared_path = Path("/shared/sync_results.json")
    try:
        shared_path.parent.mkdir(parents=True, exist_ok=True)
        shared_path.write_text(json.dumps(results, indent=2))
    except Exception:
        pass


if __name__ == "__main__":
    main()

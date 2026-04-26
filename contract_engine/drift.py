"""Schema drift detection — compares contract-declared schema vs actual OM table."""

from __future__ import annotations

import logging
from typing import Any

from models import DataContract, LogicalType

logger = logging.getLogger(__name__)

OM_TYPE_MAP: dict[str, set[str]] = {
    "string": {"VARCHAR", "TEXT", "CHAR", "STRING", "NVARCHAR", "CLOB", "MEDIUMTEXT", "LONGTEXT"},
    "integer": {"INT", "INTEGER", "SMALLINT", "TINYINT", "MEDIUMINT", "INT4", "INT2"},
    "long": {"BIGINT", "INT8", "LONG"},
    "float": {"FLOAT", "FLOAT4", "REAL"},
    "double": {"DOUBLE", "FLOAT8", "DOUBLE PRECISION", "NUMERIC", "DECIMAL"},
    "decimal": {"DECIMAL", "NUMERIC", "NUMBER"},
    "boolean": {"BOOLEAN", "BOOL", "BIT"},
    "date": {"DATE"},
    "timestamp": {"TIMESTAMP", "DATETIME", "TIMESTAMPTZ", "TIMESTAMP_NTZ", "TIMESTAMP WITH TIME ZONE"},
    "binary": {"BINARY", "VARBINARY", "BLOB", "BYTEA"},
    "array": {"ARRAY"},
    "object": {"JSON", "JSONB", "STRUCT", "MAP", "OBJECT"},
    "uuid": {"UUID"},
}


def _types_compatible(contract_type: LogicalType, om_data_type: str) -> bool:
    """Check if an OpenMetadata column type is compatible with the contract logical type."""
    normalized = om_data_type.upper().strip()
    allowed = OM_TYPE_MAP.get(contract_type.value, set())
    if normalized in allowed:
        return True
    if contract_type.value.upper() == normalized:
        return True
    return False


def detect_schema_drift(
    contract: DataContract,
    om_table: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compare contract schema declarations against an actual OpenMetadata table.

    Returns a drift report with added, removed, and changed columns.
    """
    report: dict[str, Any] = {
        "contract": contract.data_product,
        "version": contract.version,
        "drifts": [],
        "summary": {"added": 0, "removed": 0, "type_mismatch": 0, "nullability_change": 0},
        "has_drift": False,
    }

    if not om_table:
        report["drifts"].append({
            "type": "TABLE_NOT_FOUND",
            "severity": "error",
            "message": "Table does not exist in OpenMetadata",
        })
        report["has_drift"] = True
        return report

    om_columns: dict[str, dict] = {}
    for col in om_table.get("columns", []):
        col_name = col.get("name", "")
        om_columns[col_name] = col

    all_contract_col_names: set[str] = set()
    for schema_obj in contract.schema_objects:
        contract_cols = {c.name: c for c in schema_obj.properties}
        all_contract_col_names.update(contract_cols.keys())

        for cname, ccol in contract_cols.items():
            if cname not in om_columns:
                report["drifts"].append({
                    "type": "COLUMN_MISSING_IN_TABLE",
                    "severity": "error",
                    "column": cname,
                    "message": f"Contract declares column '{cname}' but it does not exist in the actual table",
                })
                report["summary"]["removed"] += 1
                continue

            om_col = om_columns[cname]
            om_type = om_col.get("dataType", "UNKNOWN")

            if not _types_compatible(ccol.logical_type, om_type):
                report["drifts"].append({
                    "type": "TYPE_MISMATCH",
                    "severity": "error",
                    "column": cname,
                    "contract_type": ccol.logical_type.value,
                    "actual_type": om_type,
                    "message": f"Column '{cname}': contract says '{ccol.logical_type.value}', actual is '{om_type}'",
                })
                report["summary"]["type_mismatch"] += 1

            om_constraint = om_col.get("constraint", "NULL")
            om_nullable = om_constraint != "NOT_NULL"
            if ccol.required and om_nullable:
                report["drifts"].append({
                    "type": "NULLABILITY_CHANGE",
                    "severity": "warning",
                    "column": cname,
                    "message": f"Column '{cname}' is required in contract but nullable in actual table",
                })
                report["summary"]["nullability_change"] += 1

    for om_name in om_columns:
        if om_name not in all_contract_col_names:
            report["drifts"].append({
                "type": "COLUMN_ADDED_NOT_IN_CONTRACT",
                "severity": "info",
                "column": om_name,
                "message": f"Column '{om_name}' exists in table but is not declared in the contract",
            })
            report["summary"]["added"] += 1

    report["has_drift"] = len(report["drifts"]) > 0
    return report

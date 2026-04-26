"""MCP Server — Data Contract Governance tools for AI agents.

Exposes MCP tools for contract validation, drift detection, and violation listing.
Includes an OpenAI-powered /chat REST endpoint for natural-language queries.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field

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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

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


# ── OpenAI Agent with function-calling ──────────────────────────

TOOL_FUNCTIONS = {
    "validate_contract": validate_contract,
    "list_violations": list_violations,
    "get_contract_status": get_contract_status,
    "detect_drift": detect_drift,
}

OPENAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "validate_contract",
            "description": "Validate a data contract against OpenMetadata. Returns compliance report.",
            "parameters": {
                "type": "object",
                "properties": {
                    "contract_name": {"type": "string", "description": "Name of the data product (e.g. 'seller-transactions')"}
                },
                "required": ["contract_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_violations",
            "description": "List all data contracts that are currently violated.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_contract_status",
            "description": "Get overall health status of a specific data contract.",
            "parameters": {
                "type": "object",
                "properties": {
                    "contract_name": {"type": "string", "description": "Name of the data product"}
                },
                "required": ["contract_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "detect_drift",
            "description": "Detect schema drift between contract declaration and actual table in OpenMetadata.",
            "parameters": {
                "type": "object",
                "properties": {
                    "contract_name": {"type": "string", "description": "Name of the data product"}
                },
                "required": ["contract_name"],
            },
        },
    },
]

SYSTEM_PROMPT = (
    "You are BlastRadius AI, a data contract governance assistant. "
    "You help data teams understand their contract compliance, detect schema drift, "
    "and find violations. You have access to tools that query OpenMetadata in real time. "
    "Always call tools to get live data before answering. Be concise and actionable."
)


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = Field(default_factory=list)


class ChatResponse(BaseModel):
    reply: str
    tool_calls: list[dict] = Field(default_factory=list)


async def _run_openai_agent(user_message: str, history: list[dict]) -> ChatResponse:
    """Run an OpenAI function-calling loop against our MCP tools."""
    try:
        from openai import OpenAI
    except ImportError:
        return ChatResponse(
            reply="OpenAI SDK not installed. Set OPENAI_API_KEY and install openai package.",
            tool_calls=[],
        )

    if not OPENAI_API_KEY:
        return ChatResponse(
            reply="OPENAI_API_KEY not configured. Set it in the environment to enable AI chat.",
            tool_calls=[],
        )

    client = OpenAI(api_key=OPENAI_API_KEY)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    tool_calls_log: list[dict] = []

    for _ in range(5):
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=OPENAI_TOOLS,
            tool_choice="auto",
        )

        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" or choice.message.tool_calls:
            messages.append(choice.message)

            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments) if tc.function.arguments else {}

                logger.info("OpenAI called tool: %s(%s)", fn_name, fn_args)
                tool_calls_log.append({"tool": fn_name, "args": fn_args})

                fn = TOOL_FUNCTIONS.get(fn_name)
                if fn:
                    result = fn(**fn_args)
                else:
                    result = json.dumps({"error": f"Unknown tool: {fn_name}"})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
        else:
            return ChatResponse(
                reply=choice.message.content or "No response generated.",
                tool_calls=tool_calls_log,
            )

    return ChatResponse(reply="Agent reached max iterations.", tool_calls=tool_calls_log)


# ── Mount /chat as a custom HTTP endpoint on the MCP app ────────

def _mount_chat_endpoint():
    """Add /chat, /health, and / (chat UI) REST endpoints alongside the MCP transport."""
    from starlette.requests import Request
    from starlette.responses import JSONResponse, HTMLResponse
    from starlette.routing import Route
    import asyncio

    chat_html_path = Path(__file__).parent / "chat.html"

    async def index_endpoint(request: Request) -> HTMLResponse:
        if chat_html_path.exists():
            return HTMLResponse(chat_html_path.read_text())
        return HTMLResponse("<h1>BlastRadius AI</h1><p>chat.html not found</p>")

    async def chat_endpoint(request: Request) -> JSONResponse:
        body = await request.json()
        req = ChatRequest(**body)
        resp = await _run_openai_agent(req.message, req.history)
        return JSONResponse(resp.model_dump())

    async def health_endpoint(request: Request) -> JSONResponse:
        om_ok = _get_client().health_check()
        return JSONResponse({
            "mcp_server": "ok",
            "openmetadata": "ok" if om_ok else "unreachable",
            "openai_configured": bool(OPENAI_API_KEY),
        })

    return [
        Route("/", index_endpoint, methods=["GET"]),
        Route("/chat", chat_endpoint, methods=["POST"]),
        Route("/health", health_endpoint, methods=["GET"]),
    ]


if __name__ == "__main__":
    logger.info("Starting MCP server on %s:%d", MCP_HOST, MCP_PORT)
    logger.info("OpenAI API: %s", "configured" if OPENAI_API_KEY else "NOT SET (chat disabled)")

    try:
        extra_routes = _mount_chat_endpoint()
        app = mcp.streamable_http_app()
        app.routes.extend(extra_routes)

        import uvicorn
        uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
    except Exception:
        logger.warning("Falling back to default MCP transport")
        mcp.run(transport="streamable-http")

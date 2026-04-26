# Data Contract Governance Engine

A governance automation engine that enforces data contracts between producer and consumer teams using OpenMetadata. Contracts are defined as YAML (ODCS v3.1.0), synced to OpenMetadata as governance policies/tags/quality tests, queryable via MCP tools, validated in CI/CD, and visualized in a compliance dashboard.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Stack                         │
│                                                                     │
│  ┌─────────┐  ┌───────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  MySQL   │  │ Elasticsearch │  │  OpenMetadata │  │  Airflow   │ │
│  │  :3306   │  │    :9200      │  │    :8585      │  │   :8080    │ │
│  └────┬─────┘  └──────┬────────┘  └──────┬───────┘  └────────────┘ │
│       │               │                  │                          │
│       └───────────────┴──────────────────┘                          │
│                          │                                          │
│              ┌───────────┼───────────┐                              │
│              ▼           ▼           ▼                               │
│  ┌──────────────┐ ┌───────────┐ ┌───────────────┐                  │
│  │Contract Sync │ │MCP Server │ │   Dashboard   │                  │
│  │   Engine     │ │  :8000    │ │    :8501      │                  │
│  └──────────────┘ └───────────┘ └───────────────┘                  │
└──────────────────────────────────────────────────────────────────────┘
         ▲                                         ▲
         │                                         │
    contracts/*.yaml                     GitHub Action (CI/CD)
    (ODCS v3.1.0)                        PR validation + blocking
```

## Quick Start

### Prerequisites

- Docker & Docker Compose v2
- Python 3.11+ (for local validation and seed scripts)
- ~8 GB RAM allocated to Docker

### 1. Start the stack

```bash
docker compose up -d --build
```

Or using Make:

```bash
make up
```

Wait 2-3 minutes for OpenMetadata to initialize. Check readiness:

```bash
make status
```

### 2. Seed sample data

```bash
make seed
```

This creates sample database services, tables, classifications, tags, and quality tests in OpenMetadata from the contracts in `contracts/`.

### 3. Run the demo

```bash
make demo
```

This simulates a producer team modifying a table schema without updating the contract, then shows the resulting drift detection.

### 4. Access the services

| Service | URL | Credentials |
|---------|-----|-------------|
| OpenMetadata UI | http://localhost:8585 | admin / admin |
| Airflow UI | http://localhost:8080 | admin / admin |
| Compliance Dashboard | http://localhost:8501 | — |
| MCP Server | http://localhost:8000/mcp | — |

## Project Structure

```
├── docker-compose.yml              # Full stack definition
├── .env                            # Environment configuration
├── Makefile                        # Dev workflow commands
├── schemas/
│   └── contract-schema.json        # ODCS v3.1.0 JSON meta-schema
├── contracts/                      # YAML data contracts
│   ├── seller-transactions.yaml
│   ├── buyer-orders.yaml
│   └── product-catalog.yaml
├── contract_engine/                # Core sync & validation engine
│   ├── Dockerfile
│   ├── models.py                   # Pydantic contract models
│   ├── om_client.py                # OpenMetadata API client
│   ├── sync.py                     # YAML -> OpenMetadata sync
│   ├── validator.py                # Validation & breaking-change detection
│   └── drift.py                    # Schema drift detection
├── mcp_server/                     # MCP tools for AI agents
│   ├── Dockerfile
│   └── server.py                   # FastMCP server (4 tools)
├── dashboard/                      # Streamlit compliance UI
│   ├── Dockerfile
│   └── app.py
├── github-action/                  # CI/CD contract validation
│   ├── action.yml
│   ├── validate.py
│   └── detect_breaking_changes.py
├── .github/workflows/
│   └── contract-validation.yml     # Example GitHub workflow
└── seed/
    ├── setup.py                    # Bootstrap script
    └── demo_violation.py           # Violation demo script
```

## Data Contract Format

Contracts follow the [Open Data Contract Standard (ODCS) v3.1.0](https://bitol-io.github.io/open-data-contract-standard/). Minimal example:

```yaml
kind: DataContract
apiVersion: v3.1.0
id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
domain: seller
dataProduct: seller-transactions
version: 1.0.0
status: active

schema:
  - name: seller_transactions
    properties:
      - name: txn_id
        logicalType: string
        required: true
      - name: amount
        logicalType: double
        required: true
        quality:
          - metric: nullValues
            mustBe: 0
            severity: error

slaProperties:
  - property: latency
    value: 4
    unit: d

team:
  name: seller-data-team
```

## MCP Tools

The MCP server exposes four tools for AI agent integration:

| Tool | Description |
|------|-------------|
| `validate_contract(contract_name)` | Full compliance check: meta-schema, quality rules, SLA |
| `list_violations()` | All currently violated contracts across all domains |
| `get_contract_status(contract_name)` | Health summary: drift, quality score, SLA status |
| `detect_drift(contract_name)` | Schema drift: added/removed columns, type mismatches |

## CI/CD Integration

The GitHub Action validates contracts on every PR that touches `contracts/`:

1. **Meta-schema validation** — YAML conforms to ODCS structure
2. **Breaking change detection** — diff against base branch
3. **Business rule checks** — team, SLA, quality rule completeness
4. **PR comment** — posts a formatted validation report
5. **Merge blocking** — fails the check if violations are found

## Local Validation

Validate a contract without Docker:

```bash
make validate CONTRACT=contracts/seller-transactions.yaml
```

## Cleanup

```bash
make clean    # Removes containers + volumes (fresh start)
make down     # Stops containers, keeps volumes
```

## Tech Stack

- **OpenMetadata 1.5.11** — Governance, classification, profiler, data quality APIs
- **Python 3.11** — Contract engine, MCP server, dashboard
- **FastMCP** — Model Context Protocol server (Streamable HTTP)
- **Streamlit** — Compliance dashboard UI
- **Plotly** — Interactive charts and gauges
- **Docker Compose** — Full stack orchestration
- **GitHub Actions** — CI/CD contract validation

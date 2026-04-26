"""Data Contract Compliance Dashboard — Streamlit UI."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

# In Docker, PYTHONPATH=/contract_engine is set via docker-compose.
# For local dev, fall back to the sibling directory.
_engine_dir = os.getenv("PYTHONPATH", "")
if not _engine_dir:
    _engine_dir = str(Path(__file__).resolve().parent.parent / "contract_engine")
if _engine_dir not in sys.path:
    sys.path.insert(0, _engine_dir)

from models import DataContract
from om_client import OpenMetadataClient
from validator import load_all_contracts, evaluate_quality_rules
from drift import detect_schema_drift

CONTRACTS_DIR = os.getenv("CONTRACTS_DIR", "/contracts")
SERVICE_NAME = "demo_postgres"


# ── Helpers (defined before use) ────────────────────────────────


def _format_constraint(rule) -> str:
    parts = []
    if rule.must_be is not None:
        parts.append(f"== {rule.must_be}")
    if rule.must_be_greater_than is not None:
        parts.append(f"> {rule.must_be_greater_than}")
    if rule.must_be_less_than is not None:
        parts.append(f"< {rule.must_be_less_than}")
    if rule.must_not_be is not None:
        parts.append(f"!= {rule.must_not_be}")
    return ", ".join(parts) if parts else "N/A"


@st.cache_resource
def get_client() -> OpenMetadataClient:
    return OpenMetadataClient()


def resolve_fqn(contract: DataContract) -> str | None:
    if not contract.server or not contract.schema_objects:
        return None
    return f"{SERVICE_NAME}.{contract.server.database}.{contract.server.schema_name}.{contract.schema_objects[0].name}"


def get_compliance_data() -> list[dict]:
    """Build compliance info for all contracts."""
    client = get_client()
    contracts = load_all_contracts(CONTRACTS_DIR)
    data = []

    for path, contract in contracts:
        fqn = resolve_fqn(contract)
        entry = {
            "Contract": contract.data_product,
            "Version": contract.version,
            "Domain": contract.domain,
            "Status": contract.status.value,
            "Team": contract.team.name if contract.team else "N/A",
            "Table FQN": fqn or "N/A",
            "Quality Score": "N/A",
            "Schema Drift": "N/A",
            "Overall": "Unknown",
        }

        if fqn:
            try:
                profile = client.get_table_profile(fqn)
                quality_results = evaluate_quality_rules(contract, profile)
                passed = sum(1 for r in quality_results if r["passed"])
                total = len(quality_results)
                entry["Quality Score"] = f"{passed}/{total}" if total > 0 else "No rules"
                quality_pct = (passed / total * 100) if total > 0 else 100

                om_table = client.get_table_by_fqn(fqn)
                drift_report = detect_schema_drift(contract, om_table)
                entry["Schema Drift"] = "Yes" if drift_report["has_drift"] else "No"

                if quality_pct == 100 and not drift_report["has_drift"]:
                    entry["Overall"] = "Compliant"
                elif quality_pct >= 80:
                    entry["Overall"] = "Warning"
                else:
                    entry["Overall"] = "Violated"
            except Exception as exc:
                entry["Overall"] = f"Error: {exc}"

        data.append(entry)
    return data


# ── Page Config ─────────────────────────────────────────────────

st.set_page_config(
    page_title="Data Contract Governance",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        text-align: center;
    }
    .metric-value { font-size: 2.5rem; font-weight: 700; }
    .metric-label { font-size: 0.9rem; opacity: 0.85; margin-top: 4px; }
    .status-compliant { color: #00c853; font-weight: 600; }
    .status-warning { color: #ffa726; font-weight: 600; }
    .status-violated { color: #ef5350; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────

with st.sidebar:
    st.title("Data Contract Governance")
    st.caption("Powered by OpenMetadata")
    st.divider()

    page = st.radio("Navigate", ["Domain Overview", "Contract Detail", "SLA Tracker", "Violations"])

    st.divider()
    st.caption("Settings")
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)

    st.divider()
    om_healthy = get_client().health_check()
    if om_healthy:
        st.success("OpenMetadata: Connected")
    else:
        st.error("OpenMetadata: Unreachable")


@st.fragment(run_every=60 if auto_refresh else None)
def _auto_refresh_trigger():
    """Triggers parent rerun every 60s when auto-refresh is enabled."""
    pass


if auto_refresh:
    _auto_refresh_trigger()

# ── Domain Overview ─────────────────────────────────────────────

if page == "Domain Overview":
    st.header("Domain Compliance Overview")

    compliance_data = get_compliance_data()
    df = pd.DataFrame(compliance_data)

    col1, col2, col3, col4 = st.columns(4)

    total = len(df)
    compliant = len(df[df["Overall"] == "Compliant"])
    warnings = len(df[df["Overall"] == "Warning"])
    violated = len(df[df["Overall"] == "Violated"])

    with col1:
        st.metric("Total Contracts", total)
    with col2:
        st.metric("Compliant", compliant, delta=None)
    with col3:
        st.metric("Warnings", warnings)
    with col4:
        st.metric("Violated", violated)

    st.divider()

    if not df.empty:
        domain_summary = df.groupby("Domain").agg(
            Total=("Contract", "count"),
            Compliant=("Overall", lambda x: (x == "Compliant").sum()),
            Warning=("Overall", lambda x: (x == "Warning").sum()),
            Violated=("Overall", lambda x: (x == "Violated").sum()),
        ).reset_index()

        domain_summary["Compliance %"] = (
            domain_summary["Compliant"] / domain_summary["Total"] * 100
        ).round(1)

        col_chart, col_table = st.columns([1, 1])

        with col_chart:
            fig = px.bar(
                domain_summary,
                x="Domain",
                y=["Compliant", "Warning", "Violated"],
                title="Contract Compliance by Domain",
                barmode="stack",
                color_discrete_map={
                    "Compliant": "#00c853",
                    "Warning": "#ffa726",
                    "Violated": "#ef5350",
                },
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                legend_title_text="Status",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_table:
            fig2 = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(compliant / total * 100, 1) if total > 0 else 0,
                title={"text": "Overall Compliance Rate"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#00c853"},
                    "steps": [
                        {"range": [0, 60], "color": "#ef535033"},
                        {"range": [60, 80], "color": "#ffa72633"},
                        {"range": [80, 100], "color": "#00c85333"},
                    ],
                },
                number={"suffix": "%"},
            ))
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                height=300,
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("All Contracts")
        st.dataframe(
            df[["Contract", "Domain", "Version", "Status", "Quality Score", "Schema Drift", "Overall"]],
            use_container_width=True,
            hide_index=True,
        )

# ── Contract Detail ─────────────────────────────────────────────

elif page == "Contract Detail":
    st.header("Contract Detail View")

    contracts = load_all_contracts(CONTRACTS_DIR)
    names = [c.data_product for _, c in contracts]

    if not names:
        st.warning("No contracts found")
    else:
        selected = st.selectbox("Select Contract", names)

        contract = None
        for _, c in contracts:
            if c.data_product == selected:
                contract = c
                break

        if contract:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Version", contract.version)
            with col2:
                st.metric("Domain", contract.domain)
            with col3:
                st.metric("Status", contract.status.value.upper())

            st.divider()

            tab1, tab2, tab3, tab4 = st.tabs(["Schema", "Quality Rules", "SLA Properties", "Team & Access"])

            with tab1:
                for schema_obj in contract.schema_objects:
                    st.subheader(f"Table: {schema_obj.name}")
                    cols_data = []
                    for col in schema_obj.properties:
                        cols_data.append({
                            "Column": col.name,
                            "Type": col.logical_type.value,
                            "Required": "Yes" if col.required else "No",
                            "PK": "Yes" if col.primary_key else "No",
                            "Classification": col.classification.value if col.classification else "N/A",
                            "Description": col.description or "",
                        })
                    st.dataframe(pd.DataFrame(cols_data), use_container_width=True, hide_index=True)

            with tab2:
                all_rules = contract.get_all_quality_rules()
                if all_rules:
                    rules_data = []
                    for col_name, rule in all_rules:
                        rules_data.append({
                            "Scope": col_name or "Table-level",
                            "Metric": rule.metric,
                            "Constraint": _format_constraint(rule),
                            "Severity": rule.severity.value,
                            "Dimension": rule.dimension.value if rule.dimension else "N/A",
                        })
                    st.dataframe(pd.DataFrame(rules_data), use_container_width=True, hide_index=True)
                else:
                    st.info("No quality rules defined")

            with tab3:
                if contract.sla_properties:
                    sla_data = [
                        {"Property": s.property, "Value": s.value, "Unit": s.unit or ""}
                        for s in contract.sla_properties
                    ]
                    st.dataframe(pd.DataFrame(sla_data), use_container_width=True, hide_index=True)
                else:
                    st.info("No SLA properties defined")

            with tab4:
                if contract.team:
                    st.subheader(f"Team: {contract.team.name}")
                    if contract.team.members:
                        members = [
                            {"Username": m.username, "Role": m.role, "Joined": m.date_in or "N/A"}
                            for m in contract.team.members
                        ]
                        st.dataframe(pd.DataFrame(members), use_container_width=True, hide_index=True)

                if contract.roles:
                    st.subheader("Access Roles")
                    roles_d = [{"Role": r.role, "Access": r.access} for r in contract.roles]
                    st.dataframe(pd.DataFrame(roles_d), use_container_width=True, hide_index=True)

# ── SLA Tracker ─────────────────────────────────────────────────

elif page == "SLA Tracker":
    st.header("SLA Tracker")

    contracts = load_all_contracts(CONTRACTS_DIR)
    sla_data = []

    for _, contract in contracts:
        for sla in contract.sla_properties:
            sla_data.append({
                "Contract": contract.data_product,
                "Domain": contract.domain,
                "Property": sla.property,
                "Value": sla.value,
                "Unit": sla.unit or "",
            })

    if sla_data:
        df = pd.DataFrame(sla_data)

        latency_df = df[df["Property"] == "latency"]
        if not latency_df.empty:
            st.subheader("Freshness SLAs (Latency)")
            fig = px.bar(
                latency_df,
                x="Contract",
                y="Value",
                color="Domain",
                title="Maximum Allowed Latency by Contract",
                labels={"Value": "Latency (days)"},
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("All SLA Properties")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No SLA properties found across contracts")

# ── Violations ──────────────────────────────────────────────────

elif page == "Violations":
    st.header("Current Violations")

    compliance_data = get_compliance_data()
    df = pd.DataFrame(compliance_data)
    violated_df = df[df["Overall"].isin(["Violated", "Warning"])]

    if violated_df.empty:
        st.success("All contracts are compliant!")
    else:
        st.error(f"{len(violated_df)} contract(s) have issues")
        st.dataframe(
            violated_df[["Contract", "Domain", "Quality Score", "Schema Drift", "Overall"]],
            use_container_width=True,
            hide_index=True,
        )

        for _, row in violated_df.iterrows():
            with st.expander(f"Details: {row['Contract']}"):
                st.json({
                    "contract": row["Contract"],
                    "domain": row["Domain"],
                    "quality_score": row["Quality Score"],
                    "schema_drift": row["Schema Drift"],
                    "status": row["Overall"],
                })

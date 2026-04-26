"""Lightweight OpenMetadata API client using httpx (no SDK dependency for portability)."""

from __future__ import annotations

import base64
import logging
import os
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
_TOKEN_TTL = 3500  # refresh ~100s before 1h expiry


class OpenMetadataClient:
    """Thin wrapper around the OpenMetadata REST API v1."""

    def __init__(
        self,
        base_url: str | None = None,
        jwt_token: str | None = None,
        admin_user: str | None = None,
        admin_password: str | None = None,
        timeout: float = 30.0,
    ):
        self.base_url = (base_url or os.getenv("OM_SERVER_URL", "http://localhost:8585/api")).rstrip("/")
        self._jwt_token = jwt_token or os.getenv("OM_JWT_TOKEN", "")
        self._admin_user = admin_user or os.getenv("OM_ADMIN_USER", "admin@openmetadata.org")
        self._admin_password = admin_password or os.getenv("OM_ADMIN_PASSWORD", "admin")
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _get_token(self) -> str:
        if self._jwt_token:
            return self._jwt_token

        cached = _TOKEN_CACHE.get(self.base_url)
        if cached and (time.time() - cached[1]) < _TOKEN_TTL:
            return cached[0]

        pw_b64 = base64.b64encode(self._admin_password.encode()).decode()
        resp = self._client.post(
            "/v1/users/login",
            json={"email": self._admin_user, "password": pw_b64},
        )
        resp.raise_for_status()
        body = resp.json()
        token = body.get("accessToken", "")
        if not token:
            raise ValueError(f"No accessToken in login response: {list(body.keys())}")
        _TOKEN_CACHE[self.base_url] = (token, time.time())
        return token

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def health_check(self) -> bool:
        try:
            resp = self._client.get("/v1/system/version")
            return resp.status_code == 200
        except Exception:
            return False

    # ── Generic CRUD ────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> Any:
        resp = self._client.get(path, headers=self._headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, payload: dict) -> Any:
        resp = self._client.put(path, headers=self._headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict) -> Any:
        resp = self._client.post(path, headers=self._headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, payload: list[dict]) -> Any:
        headers = {**self._headers, "Content-Type": "application/json-patch+json"}
        resp = self._client.patch(path, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> None:
        resp = self._client.delete(path, headers=self._headers)
        resp.raise_for_status()

    # ── Classifications & Tags ──────────────────────────────────

    def create_or_update_classification(self, name: str, description: str = "") -> dict:
        payload = {
            "name": name,
            "description": description,
            "mutuallyExclusive": False,
        }
        return self._put("/v1/classifications", payload)

    def create_or_update_tag(
        self, classification: str, name: str, description: str = ""
    ) -> dict:
        payload = {
            "classification": classification,
            "name": name,
            "description": description,
        }
        return self._put("/v1/tags", payload)

    # ── Tables ──────────────────────────────────────────────────

    def get_table_by_fqn(self, fqn: str, fields: str = "columns,tags") -> Optional[dict]:
        try:
            return self._get(f"/v1/tables/name/{fqn}", params={"fields": fields})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (404, 400):
                return None
            raise

    def list_tables(self, limit: int = 100, fields: str = "columns,tags") -> list[dict]:
        data = self._get("/v1/tables", params={"limit": limit, "fields": fields})
        return data.get("data", [])

    def add_tag_to_table(self, table_id: str, tag_fqn: str) -> dict:
        patch_ops = [
            {
                "op": "add",
                "path": "/tags/-",
                "value": {
                    "tagFQN": tag_fqn,
                    "source": "Classification",
                    "labelType": "Automated",
                    "state": "Confirmed",
                },
            }
        ]
        return self._patch(f"/v1/tables/{table_id}", patch_ops)

    # ── Database Services ───────────────────────────────────────

    def create_database_service(self, name: str, service_type: str = "Postgres") -> dict:
        payload = {
            "name": name,
            "serviceType": service_type,
            "connection": {
                "config": {
                    "type": service_type,
                    "hostPort": "host.docker.internal:5432",
                    "username": "demo",
                    "authType": {"password": "demo"},
                    "database": "demo_db",
                }
            },
        }
        return self._put("/v1/services/databaseServices", payload)

    def create_database(self, service_fqn: str, name: str) -> dict:
        payload = {
            "name": name,
            "service": service_fqn,
        }
        return self._put("/v1/databases", payload)

    def create_database_schema(self, database_fqn: str, name: str) -> dict:
        payload = {
            "name": name,
            "database": database_fqn,
        }
        return self._put("/v1/databaseSchemas", payload)

    def create_table(self, schema_fqn: str, name: str, columns: list[dict]) -> dict:
        payload = {
            "name": name,
            "databaseSchema": schema_fqn,
            "columns": columns,
        }
        return self._put("/v1/tables", payload)

    # ── Policies ────────────────────────────────────────────────

    def create_or_update_policy(self, name: str, rules: list[dict], description: str = "") -> dict:
        payload = {
            "name": name,
            "description": description,
            "rules": rules,
            "enabled": True,
        }
        return self._put("/v1/policies", payload)

    # ── Data Quality ────────────────────────────────────────────

    def create_or_update_test_suite(self, name: str, description: str = "") -> dict:
        payload = {
            "name": name,
            "description": description,
            "executableEntityReference": name,
        }
        try:
            return self._put("/v1/dataQuality/testSuites", payload)
        except httpx.HTTPStatusError:
            payload.pop("executableEntityReference", None)
            return self._put("/v1/dataQuality/testSuites", payload)

    def create_test_case(
        self,
        test_suite_fqn: str,
        table_fqn: str,
        name: str,
        test_type: str,
        params: dict | None = None,
        column_name: str | None = None,
    ) -> dict:
        entity_link = f"<#E::table::{table_fqn}>"
        if column_name:
            entity_link = f"<#E::table::{table_fqn}::columns::{column_name}>"

        payload = {
            "name": name,
            "testDefinition": test_type,
            "entityLink": entity_link,
            "testSuite": test_suite_fqn,
            "parameterValues": [
                {"name": k, "value": str(v)} for k, v in (params or {}).items()
            ],
        }
        return self._put("/v1/dataQuality/testCases", payload)

    def list_test_cases(self, table_fqn: str) -> list[dict]:
        entity_link = f"<#E::table::{table_fqn}>"
        try:
            data = self._get(
                "/v1/dataQuality/testCases",
                params={"entityLink": entity_link, "limit": 100},
            )
            return data.get("data", [])
        except httpx.HTTPStatusError:
            return []

    def get_table_profile(self, table_fqn: str) -> Optional[dict]:
        table = self.get_table_by_fqn(table_fqn, fields="tableProfile")
        if table:
            return table.get("profile")
        return None

    # ── Search ──────────────────────────────────────────────────

    def search(self, query: str, index: str = "table_search_index", size: int = 10) -> list[dict]:
        data = self._get(
            "/v1/search/query",
            params={"q": query, "index": index, "size": size},
        )
        hits = data.get("hits", {}).get("hits", [])
        return [h.get("_source", {}) for h in hits]

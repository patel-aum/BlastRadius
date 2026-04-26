"""Pydantic models for parsing and validating ODCS v3.1.0 data contracts."""

from __future__ import annotations

import uuid
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class ContractStatus(str, Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    DRAFT = "draft"


class LogicalType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    ARRAY = "array"
    OBJECT = "object"
    LONG = "long"
    UUID = "uuid"


class Classification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class QualityDimension(str, Enum):
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class QualityRule(BaseModel):
    metric: str
    must_be: Any = Field(default=None, alias="mustBe")
    must_be_greater_than: Optional[float] = Field(default=None, alias="mustBeGreaterThan")
    must_be_less_than: Optional[float] = Field(default=None, alias="mustBeLessThan")
    must_be_greater_than_or_equal_to: Optional[float] = Field(
        default=None, alias="mustBeGreaterThanOrEqualTo"
    )
    must_be_less_than_or_equal_to: Optional[float] = Field(
        default=None, alias="mustBeLessThanOrEqualTo"
    )
    must_not_be: Any = Field(default=None, alias="mustNotBe")
    type: str = "library"
    description: Optional[str] = None
    dimension: Optional[QualityDimension] = None
    severity: Severity = Severity.ERROR
    unit: Optional[str] = None
    schedule: Optional[str] = None
    scheduler: Optional[str] = None

    model_config = {"populate_by_name": True}


class ColumnProperty(BaseModel):
    name: str
    physical_name: Optional[str] = Field(default=None, alias="physicalName")
    logical_type: LogicalType = Field(alias="logicalType")
    physical_type: Optional[str] = Field(default=None, alias="physicalType")
    required: bool = False
    primary_key: bool = Field(default=False, alias="primaryKey")
    partitioned: bool = False
    classification: Optional[Classification] = None
    description: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    examples: list[Any] = Field(default_factory=list)
    quality: list[QualityRule] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class SchemaObject(BaseModel):
    name: str
    physical_name: Optional[str] = Field(default=None, alias="physicalName")
    physical_type: Optional[str] = Field(default=None, alias="physicalType")
    description: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    properties: list[ColumnProperty]
    quality: list[QualityRule] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class SLAProperty(BaseModel):
    property: str
    value: Any
    unit: Optional[str] = None
    element: Optional[str] = None
    driver: Optional[str] = None


class TeamMember(BaseModel):
    username: str
    role: str
    date_in: Optional[str] = Field(default=None, alias="dateIn")

    model_config = {"populate_by_name": True}


class Team(BaseModel):
    name: str
    description: Optional[str] = None
    members: list[TeamMember] = Field(default_factory=list)


class AccessRole(BaseModel):
    role: str
    access: str
    first_level_approvers: Optional[str] = Field(default=None, alias="firstLevelApprovers")
    second_level_approvers: Optional[str] = Field(default=None, alias="secondLevelApprovers")

    model_config = {"populate_by_name": True}


class ContractDescription(BaseModel):
    purpose: Optional[str] = None
    limitations: Optional[str] = None
    usage: Optional[str] = None


class ServerConfig(BaseModel):
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: str
    schema_name: str = Field(alias="schema")

    model_config = {"populate_by_name": True}


class SupportChannel(BaseModel):
    channel: Optional[str] = None
    tool: Optional[str] = None
    url: Optional[str] = None


class CustomProperty(BaseModel):
    property: str
    value: Any


class DataContract(BaseModel):
    """Root model for an ODCS v3.1.0 data contract."""

    kind: str = "DataContract"
    api_version: str = Field(alias="apiVersion")
    id: uuid.UUID
    domain: str
    data_product: str = Field(alias="dataProduct")
    version: str
    status: ContractStatus

    description: Optional[ContractDescription] = None
    server: Optional[ServerConfig] = None
    schema_objects: list[SchemaObject] = Field(alias="schema")
    sla_properties: list[SLAProperty] = Field(default_factory=list, alias="slaProperties")
    quality: list[QualityRule] = Field(default_factory=list)
    team: Optional[Team] = None
    roles: list[AccessRole] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    custom_properties: list[CustomProperty] = Field(
        default_factory=list, alias="customProperties"
    )
    support: list[SupportChannel] = Field(default_factory=list)
    contract_created_ts: Optional[str] = Field(default=None, alias="contractCreatedTs")

    model_config = {"populate_by_name": True}

    @property
    def table_fqn_hint(self) -> str:
        """Best-guess FQN fragment: database.schema.table."""
        if self.server and self.schema_objects:
            return f"{self.server.database}.{self.server.schema_name}.{self.schema_objects[0].name}"
        return self.data_product

    def get_all_quality_rules(self) -> list[tuple[str | None, QualityRule]]:
        """Collect all quality rules: table-level (None) and column-level (col name)."""
        rules: list[tuple[str | None, QualityRule]] = []
        for rule in self.quality:
            rules.append((None, rule))
        for schema_obj in self.schema_objects:
            for rule in schema_obj.quality:
                rules.append((None, rule))
            for col in schema_obj.properties:
                for rule in col.quality:
                    rules.append((col.name, rule))
        return rules

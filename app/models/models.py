"""
models.py  –  All Pydantic data models used across the system.

Key design:  The StructuredQuery is the contract between the NLP parser
and the deterministic query engine.  Once a query is parsed into this
structure, NO AI is involved in producing the final answer.
"""
from __future__ import annotations
from enum import Enum
from typing import Any, List, Literal, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class FilterOperator(str, Enum):
    EQ          = "eq"           # ==
    NEQ         = "neq"          # !=
    GT          = "gt"           # >
    GTE         = "gte"          # >=
    LT          = "lt"           # <
    LTE         = "lte"          # <=
    CONTAINS    = "contains"     # substring / list membership
    NOT_CONTAINS= "not_contains"
    IN          = "in"           # value in list
    NOT_IN      = "not_in"


class AggregationType(str, Enum):
    LIST        = "list"         # return matching rows
    COUNT       = "count"        # count of rows
    SUM         = "sum"          # sum of a numeric column
    AVERAGE     = "average"      # mean of a numeric column
    PERCENTAGE  = "percentage"   # (part / whole) * 100
    MIN         = "min"
    MAX         = "max"


class OutputFormat(str, Enum):
    SINGLE_VALUE = "single_value"   # one number / string
    LIST         = "list"           # bullet list of rows
    TABLE        = "table"          # tabular (used in rich clients)
    SUMMARY      = "summary"        # e.g. "3 customers, total ₹45L"


# ─────────────────────────────────────────────────────────────────────────────
# Structured Query (output of NLP parser, input of query engine)
# ─────────────────────────────────────────────────────────────────────────────

class FilterCondition(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    field: str                  # canonical field name, e.g. "payment_percent"
    operator: FilterOperator
    value: Any                  # scalar or list depending on operator


class StructuredQuery(BaseModel):
    """
    Robust machine-readable query representation.
    Handles imperfect LLM outputs safely.
    """

    intent: str = Field(
        default="unknown",
        description="Short human-readable intent label"
    )

    sheet_tab: Optional[str] = Field(
        default=None,
        description="Exact worksheet tab name"
    )

    filters: List[FilterCondition] = Field(
        default_factory=list,
        description="WHERE-style conditions"
    )

    aggregation: AggregationType = Field(
        default=AggregationType.LIST
    )

    display_fields: List[str] = Field(
        default_factory=list
    )

    target_field: Optional[str] = None
    numerator_field: Optional[str] = None
    denominator_field: Optional[str] = None

    output_format: OutputFormat = OutputFormat.LIST

    raw_question: str = ""

    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0
    )

    clarification_needed: bool = False

    clarification_message: str = ""

    model_config = ConfigDict(
        use_enum_values=True,
        extra="ignore",  # Ignore unexpected LLM fields
        str_strip_whitespace=True  # Auto-trim strings
    )

    # 🔥 VALIDATORS (IMPORTANT)

    @field_validator("clarification_message", mode="before")
    @classmethod
    def fix_clarification_message(cls, v):
        return v or ""

    @field_validator("intent", mode="before")
    @classmethod
    def fix_intent(cls, v):
        return v or "unknown"

    @field_validator("sheet_tab", mode="before")
    @classmethod
    def clean_sheet_tab(cls, v):
        if not v:
            return None
        return v.strip()

    @field_validator("display_fields", mode="before")
    @classmethod
    def ensure_list(cls, v):
        return v or []

    @field_validator("filters", mode="before")
    @classmethod
    def ensure_filters(cls, v):
        return v or []

    @field_validator("confidence", mode="before")
    @classmethod
    def safe_confidence(cls, v):
        try:
            return float(v)
        except:
            return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Query Result
# ─────────────────────────────────────────────────────────────────────────────

class QueryResult(BaseModel):
    """Final result returned to the bot layer for formatting."""
    success: bool = True
    structured_query: StructuredQuery | None = None

    # Scalar results
    scalar_value: float | int | str | None = None
    scalar_label: str = ""       # e.g. "Payment received"

    # Row-level results
    rows: list[dict] = Field(default_factory=list)
    total_rows_matched: int = 0

    # Metadata
    execution_time_ms: float = 0.0
    sheet_last_refreshed: str = ""
    error_message: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Bot message models
# ─────────────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    platform: str 
    user_id: str

class IncomingMessage(BaseModel):
    platform: Literal["telegram", "whatsapp"]
    user_id: str
    username: str = ""
    text: str
    message_id: str = ""


class OutgoingMessage(BaseModel):
    platform: Literal["telegram", "whatsapp"]
    user_id: str
    text: str
    parse_mode: str = "Markdown"   # Telegram: Markdown | HTML
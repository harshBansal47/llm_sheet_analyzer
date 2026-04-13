"""
models.py  –  All Pydantic data models used across the system.

Key design:  The StructuredQuery is the contract between the NLP parser
and the deterministic query engine.  Once a query is parsed into this
structure, NO AI is involved in producing the final answer.
"""
from __future__ import annotations
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field, ConfigDict


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
    The machine-readable representation of a user's natural language question.
    This is produced by the NLP layer and consumed exclusively by the
    deterministic query engine.  No AI touches the data after this point.
    """
    intent: str = Field(
        description="Short human-readable intent label, e.g. 'payment_percent_for_customer'"
    )
    # Which tab/sheet to query.  The parser picks this from the live schema.
    # None means "search across all tabs" (used when the tab is ambiguous).
    sheet_tab: str | None = Field(
        default=None,
        description="Exact worksheet tab name to query, as it appears in the spreadsheet"
    )
    filters: list[FilterCondition] = Field(
        default_factory=list,
        description="All WHERE-style conditions to apply before aggregation"
    )
    aggregation: AggregationType = Field(
        default=AggregationType.LIST,
        description="What to compute on the filtered rows"
    )
    # Fields to show in list/table output
    display_fields: list[str] = Field(
        default_factory=list,
        description="Column names to include in the response (use exact sheet column names)"
    )
    # For SUM / AVERAGE / PERCENTAGE aggregations
    target_field: str | None = Field(
        default=None,
        description="The numeric column to aggregate (exact sheet column name)"
    )
    # For PERCENTAGE: numerator field and denominator field
    numerator_field: str | None = None
    denominator_field: str | None = None

    output_format: OutputFormat = OutputFormat.LIST

    # Raw user question – stored for logging / audit
    raw_question: str = ""
    confidence: float = Field(
        default=1.0,
        ge=0.0, le=1.0,
        description="Parser confidence (0-1). Queries below 0.7 trigger clarification."
    )
    clarification_needed: bool = False
    clarification_message: str = ""

    model_config = ConfigDict(use_enum_values=True)


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
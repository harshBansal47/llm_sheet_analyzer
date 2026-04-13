"""
services/query_parser.py  –  Natural Language → StructuredQuery

The ONLY place AI is used.  The LLM does NOT see any row data and does NOT
produce answers.  Its sole job: parse the user's question into a structured
JSON object (StructuredQuery) which the deterministic engine executes.

Key change from v1: the system prompt is built DYNAMICALLY from the live
schema returned by SheetsService.get_schema().  This means:
  • Any new tab added to the spreadsheet is immediately available to the parser
  • Any column rename is picked up on the next cache refresh
  • Zero hardcoded column or tab names anywhere in this file

Architecture guarantee (unchanged):
  User question
    → [OpenAI: only sees question + schema skeleton, never row data]
    → StructuredQuery (JSON with exact tab + column names from schema)
    → [Python/Pandas deterministic engine]
    → Answer from real data
"""
from __future__ import annotations
import json
import time
from openai import AsyncOpenAI
from app.models.models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType, OutputFormat
)
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Prompt builder  –  schema-aware, generated fresh for every parse call
# ─────────────────────────────────────────────────────────────────────────────

def _build_system_prompt(schema: dict[str, dict[str, str]]) -> str:
    """
    Build the full parser system prompt by embedding the live schema.

    schema format:  { "Tab Name": { "Column A": "numeric", "Col B": "text" } }
    """

    # ── 1. Format schema as readable block ───────────────────────────────────
    schema_lines: list[str] = []
    for tab_name, columns in schema.items():
        schema_lines.append(f'\nSheet tab: "{tab_name}"')
        for col_name, col_type in columns.items():
            schema_lines.append(f'  - "{col_name}" ({col_type})')

    schema_block = "\n".join(schema_lines)
    tab_list     = ", ".join(f'"{t}"' for t in schema.keys())

    return f"""You are a query parser for a business data tracking system backed by Google Sheets.
Your ONLY job is to convert a natural language question into a structured JSON query.
You do NOT have access to actual row data. You do NOT answer questions.
You ONLY produce a structured JSON representation of what the user is asking for.

═══════════════════════════════════════════════════════
LIVE DATABASE SCHEMA  (auto-generated from the spreadsheet)
═══════════════════════════════════════════════════════
{schema_block}

Available sheet tabs: {tab_list}
═══════════════════════════════════════════════════════

FIELD NAME RULES:
- Use the EXACT column name as it appears above (case-sensitive, spaces included).
- Use the EXACT tab name as it appears above in the "sheet_tab" field.
- Never invent column or tab names not listed above.

FILTER OPERATORS available:
  eq, neq, gt, gte, lt, lte, contains, not_contains, in, not_in

AGGREGATION TYPES available:
  list      → return matching rows
  count     → count of matching rows
  sum       → total of a numeric column
  average   → mean of a numeric column
  percentage → (numerator_field sum / denominator_field sum) × 100
  min, max  → extreme value of a numeric column

OUTPUT FORMAT types:
  single_value, list, table, summary

JSON OUTPUT SCHEMA (return this and nothing else):
{{
  "intent":              string,           // short label e.g. "payment_percent_for_customer"
  "sheet_tab":           string | null,    // EXACT tab name from schema, or null if ambiguous
  "filters": [
    {{"field": string, "operator": string, "value": any}}
  ],
  "aggregation":         string,
  "display_fields":      [string],         // exact column names to show; empty = all columns
  "target_field":        string | null,    // for sum/average/min/max
  "numerator_field":     string | null,    // for percentage
  "denominator_field":   string | null,    // for percentage
  "output_format":       string,
  "confidence":          float (0.0–1.0),
  "clarification_needed": bool,
  "clarification_message": string
}}

PARSING RULES:
1.  Always output valid JSON matching the schema above exactly.
2.  Use confidence (0.0–1.0) for how certain you are about the parse.
3.  If the question clearly refers to one tab, set sheet_tab to that tab name.
    If multiple tabs could apply, set sheet_tab=null (engine will search all).
4.  Numbers in filter values must be numeric (not strings).
5.  Text filter values should preserve the natural casing the user provided.
    For "contains" / "not_contains" operators the engine is case-insensitive.
6.  For "how many …": use aggregation=count.
7.  For "total / sum of …": use aggregation=sum, set target_field.
8.  For "what percent of X is paid": use aggregation=percentage,
    set numerator_field and denominator_field.
9.  For "list / show / find …": use aggregation=list.
10. If the question is ambiguous or references something not in the schema,
    set clarification_needed=true and explain in clarification_message.
11. display_fields should contain ONLY columns that answer the question.
    For list queries, include identifying columns plus the queried columns.
"""


# ─────────────────────────────────────────────────────────────────────────────
# QueryParser
# ─────────────────────────────────────────────────────────────────────────────

class QueryParser:
    def __init__(self):
        self._settings = get_settings()
        self._client   = AsyncOpenAI(api_key=self._settings.openai_api_key)

    async def parse(self, question: str, schema: dict[str, dict[str, str]]) -> StructuredQuery:
        """
        Convert a natural language question into a StructuredQuery.

        `schema` is passed in by the orchestrator from SheetsService.get_schema()
        so the parser always reflects the live spreadsheet structure.

        Raises ValueError if the LLM returns unparseable output.
        """
        t0 = time.monotonic()
        system_prompt = _build_system_prompt(schema)

        try:
            response = await self._client.chat.completions.create(
                model=self._settings.openai_model,
                temperature=0,           # deterministic parsing
                max_tokens=800,          # slightly more for multi-tab schema
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": f"Parse this query: {question}"},
                ],
            )
            raw_json = response.choices[0].message.content
            elapsed  = round((time.monotonic() - t0) * 1000, 1)
            logger.info("nlp_parsed", ms=elapsed, question=question[:80])

        except Exception as exc:
            logger.error("openai_error", error=str(exc))
            raise ValueError(f"Failed to parse query: {exc}") from exc

        return self._build_query(raw_json, question, schema)

    # ──────────────────────────────────────────────────────────────────────────

    def _build_query(
        self,
        raw_json: str,
        original_question: str,
        schema: dict[str, dict[str, str]],
    ) -> StructuredQuery:
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM returned invalid JSON: {exc}") from exc

        # Build filter conditions
        filters = []
        for f in data.get("filters", []):
            try:
                op = FilterOperator(f["operator"])
            except ValueError:
                op = FilterOperator.EQ
            filters.append(FilterCondition(
                field=f["field"],
                operator=op,
                value=f["value"],
            ))

        # Aggregation
        try:
            agg = AggregationType(data.get("aggregation", "list"))
        except ValueError:
            agg = AggregationType.LIST

        # Output format
        try:
            fmt = OutputFormat(data.get("output_format", "list"))
        except ValueError:
            fmt = OutputFormat.LIST

        # Validate sheet_tab against known tabs
        sheet_tab = data.get("sheet_tab")
        if sheet_tab and sheet_tab not in schema:
            # LLM hallucinated a tab name — fall back to None (search all)
            logger.warning("parser_invalid_tab", tab=sheet_tab, known=list(schema.keys()))
            sheet_tab = None

        # Smart default display_fields when LLM returns empty list
        display_fields = data.get("display_fields") or []
        if not display_fields and sheet_tab and sheet_tab in schema:
            display_fields = self._default_display_fields(schema[sheet_tab], filters, agg)

        return StructuredQuery(
            intent=data.get("intent", "unknown"),
            sheet_tab=sheet_tab,
            filters=filters,
            aggregation=agg,
            display_fields=display_fields,
            target_field=data.get("target_field"),
            numerator_field=data.get("numerator_field"),
            denominator_field=data.get("denominator_field"),
            output_format=fmt,
            raw_question=original_question,
            confidence=float(data.get("confidence", 1.0)),
            clarification_needed=bool(data.get("clarification_needed", False)),
            clarification_message=data.get("clarification_message", ""),
        )

    def _default_display_fields(
        self,
        tab_schema: dict[str, str],
        filters: list[FilterCondition],
        agg: AggregationType,
    ) -> list[str]:
        """
        When the LLM omits display_fields, infer sensible defaults:
        - For list/table: all columns in this tab (the engine will show them all)
        - For aggregations: only the target/queried column
        """
        if agg in (AggregationType.LIST.value, AggregationType.TABLE.value, "list", "table"):
            return list(tab_schema.keys())
        return []


# ─────────────────────────────────────────────────────────────────────────────
_parser: QueryParser | None = None


def get_query_parser() -> QueryParser:
    global _parser
    if _parser is None:
        _parser = QueryParser()
    return _parser
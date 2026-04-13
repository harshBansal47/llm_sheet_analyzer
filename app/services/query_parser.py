"""
services/query_parser.py  –  Natural Language → StructuredQuery

The ONLY place AI/LLM is used.  The provider (OpenAI / Anthropic / Google)
is selected from config.api_provider and resolved through the LLMClient
interface.  The parser itself is completely provider-agnostic — switching
providers requires only changing API_PROVIDER in .env.

Architecture guarantee (unchanged regardless of provider):
  User question
    → [LLMClient: only sees question + live schema, never row data]
    → StructuredQuery JSON
    → [Python/Pandas deterministic engine]
    → Answer from real sheet data
"""
from __future__ import annotations
import json
import time

from models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType, OutputFormat,
)
from services.llm_client import get_llm_client
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic system prompt  (unchanged from v2 — provider-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

def _build_system_prompt(schema: dict[str, dict[str, str]]) -> str:
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
- Use the EXACT tab name in the "sheet_tab" field.
- Never invent column or tab names not listed above.

FILTER OPERATORS: eq, neq, gt, gte, lt, lte, contains, not_contains, in, not_in
AGGREGATION TYPES: list, count, sum, average, percentage, min, max
OUTPUT FORMATS: single_value, list, table, summary

JSON OUTPUT SCHEMA — return this and ONLY this, no preamble:
{{
  "intent":               string,
  "sheet_tab":            string | null,
  "filters":              [{{"field": string, "operator": string, "value": any}}],
  "aggregation":          string,
  "display_fields":       [string],
  "target_field":         string | null,
  "numerator_field":      string | null,
  "denominator_field":    string | null,
  "output_format":        string,
  "confidence":           float (0.0–1.0),
  "clarification_needed": bool,
  "clarification_message": string
}}

RULES:
1.  Output valid JSON only — no markdown fences, no explanation text.
2.  confidence = how certain you are (0.0–1.0).
3.  Set sheet_tab to the exact tab name when clear; null if ambiguous.
4.  Filter values that are numbers must be numeric type, not strings.
5.  "how many …"      → aggregation=count
6.  "total / sum …"   → aggregation=sum,  set target_field
7.  "what percent …"  → aggregation=percentage, set numerator_field + denominator_field
8.  "list / show …"   → aggregation=list
9.  Ambiguous query   → clarification_needed=true, explain in clarification_message
"""


# ─────────────────────────────────────────────────────────────────────────────
# QueryParser
# ─────────────────────────────────────────────────────────────────────────────

class QueryParser:

    async def parse(
        self,
        question: str,
        schema: dict[str, dict[str, str]],
    ) -> StructuredQuery:
        """
        Convert a natural language question into a StructuredQuery.
        Uses whichever LLM provider is configured — transparent to callers.
        """
        t0            = time.monotonic()
        system_prompt = _build_system_prompt(schema)
        user_prompt   = f"Parse this query into JSON: {question}"

        client = get_llm_client()
        try:
            raw_json = await client.complete(system_prompt, user_prompt)
            elapsed  = round((time.monotonic() - t0) * 1000, 1)
            logger.info(
                "nlp_parsed",
                provider=client.provider_name,
                ms=elapsed,
                question=question[:80],
            )
        except Exception as exc:
            logger.error("llm_error", provider=client.provider_name, error=str(exc))
            raise ValueError(f"LLM call failed ({client.provider_name}): {exc}") from exc

        return self._build_query(raw_json, question, schema)

    # ──────────────────────────────────────────────────────────────────────────

    def _build_query(
        self,
        raw_json: str,
        original_question: str,
        schema: dict[str, dict[str, str]],
    ) -> StructuredQuery:
        # Some providers may still wrap output in fences despite instructions
        raw_json = raw_json.strip()
        if raw_json.startswith("```"):
            lines    = raw_json.splitlines()
            raw_json = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()

        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM returned invalid JSON: {exc}\nRaw: {raw_json[:200]}") from exc

        # Filters
        filters = []
        for f in data.get("filters", []):
            try:
                op = FilterOperator(f["operator"])
            except ValueError:
                op = FilterOperator.EQ
            filters.append(FilterCondition(field=f["field"], operator=op, value=f["value"]))

        # Aggregation + format
        try:
            agg = AggregationType(data.get("aggregation", "list"))
        except ValueError:
            agg = AggregationType.LIST

        try:
            fmt = OutputFormat(data.get("output_format", "list"))
        except ValueError:
            fmt = OutputFormat.LIST

        # Validate sheet_tab against known tabs
        sheet_tab = data.get("sheet_tab")
        if sheet_tab and sheet_tab not in schema:
            logger.warning("parser_hallucinated_tab", tab=sheet_tab, known=list(schema.keys()))
            sheet_tab = None

        # Default display_fields
        display_fields = data.get("display_fields") or []
        if not display_fields and sheet_tab and sheet_tab in schema:
            if agg in (AggregationType.LIST, AggregationType.TABLE, "list", "table"):
                display_fields = list(schema[sheet_tab].keys())

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


# ─────────────────────────────────────────────────────────────────────────────
_parser: QueryParser | None = None


def get_query_parser() -> QueryParser:
    global _parser
    if _parser is None:
        _parser = QueryParser()
    return _parser
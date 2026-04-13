"""
services/query_parser.py  –  Natural Language → StructuredQuery

This is the ONLY place AI is used.  The LLM does NOT see any sheet data
and does NOT produce any answers.  Its sole job is to parse the user's
question into a structured JSON object (StructuredQuery) which is then
executed deterministically by the query engine.

Architecture guarantee:
  Input  →  [OpenAI]  →  StructuredQuery (JSON)  →  [Python/Pandas]  →  Answer
               ↑ AI here only                              ↑ No AI here
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
# System prompt injected into every parser call
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a query parser for a real-estate payment tracking database.
Your ONLY job is to convert a natural language question into a structured JSON query.
You do NOT have access to actual data. You do NOT answer questions.
You ONLY produce a structured representation of what the user is asking for.

Available canonical field names (use EXACTLY these names):
  - customer_name    : Name of the customer/buyer
  - unit             : Apartment/unit identifier (e.g. A-302)
  - phase            : Project phase (e.g. Phase 1, Phase 2)
  - total_cost       : Total cost of the unit (numeric)
  - amount_received  : Amount received/paid so far (numeric)
  - payment_percent  : Percentage of total cost paid (numeric, 0-100)
  - status           : Payment/legal status (e.g. Active, Court Case, Defaulter)
  - remarks          : Free-text remarks

Available filter operators:
  eq, neq, gt, gte, lt, lte, contains, not_contains, in, not_in

Available aggregation types:
  list, count, sum, average, percentage, min, max

Output format types:
  single_value, list, table, summary

Rules:
1. Always output valid JSON matching the schema exactly.
2. Use confidence (0.0-1.0) to indicate how certain you are.
3. If the question is ambiguous, set clarification_needed=true.
4. For "what percent has been received for customer X": use aggregation=list, display payment_percent for that customer.
5. For "customers who paid more than 70%": filter payment_percent > 70, aggregation=list.
6. For "total cost and received amount for Unit X": filter unit=X, aggregation=list, display total_cost + amount_received.
7. For "how many customers in phase 1...": aggregation=count.
8. For "customers with court cases": filter status contains "court" or status eq "Court Case".
9. Numbers in filters should be numeric (not strings).
10. Phase values like "Phase 2" should match exactly including the word "Phase".
"""

PARSE_SCHEMA = {
    "type": "object",
    "properties": {
        "intent": {"type": "string"},
        "filters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field":    {"type": "string"},
                    "operator": {"type": "string"},
                    "value":    {}
                },
                "required": ["field", "operator", "value"]
            }
        },
        "aggregation":       {"type": "string"},
        "display_fields":    {"type": "array", "items": {"type": "string"}},
        "target_field":      {"type": ["string", "null"]},
        "numerator_field":   {"type": ["string", "null"]},
        "denominator_field": {"type": ["string", "null"]},
        "output_format":     {"type": "string"},
        "confidence":        {"type": "number"},
        "clarification_needed": {"type": "boolean"},
        "clarification_message": {"type": "string"}
    },
    "required": ["intent", "filters", "aggregation", "confidence"]
}


class QueryParser:
    def __init__(self):
        self._settings = get_settings()
        self._client = AsyncOpenAI(api_key=self._settings.openai_api_key)

    async def parse(self, question: str) -> StructuredQuery:
        """
        Convert a natural language question into a StructuredQuery.
        Raises ValueError if the LLM returns unparseable output.
        """
        t0 = time.monotonic()

        try:
            response = await self._client.chat.completions.create(
                model=self._settings.openai_model,
                temperature=0,          # deterministic parsing
                max_tokens=600,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": f"Parse this query: {question}"}
                ],
            )
            raw_json = response.choices[0].message.content
            elapsed = (time.monotonic() - t0) * 1000
            logger.info("nlp_parsed", ms=round(elapsed, 1), question=question[:80])

        except Exception as exc:
            logger.error("openai_error", error=str(exc))
            raise ValueError(f"Failed to parse query: {exc}") from exc

        return self._build_query(raw_json, question)

    # ──────────────────────────────────────────────────────────────────────────

    def _build_query(self, raw_json: str, original_question: str) -> StructuredQuery:
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

        # Default display_fields if empty
        display_fields = data.get("display_fields", [])
        if not display_fields:
            display_fields = self._infer_display_fields(filters, agg)

        return StructuredQuery(
            intent=data.get("intent", "unknown"),
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

    def _infer_display_fields(
        self,
        filters: list[FilterCondition],
        agg: AggregationType
    ) -> list[str]:
        """
        Smart default: always show customer_name + unit + relevant fields.
        """
        base = ["customer_name", "unit"]
        if agg in (AggregationType.LIST, AggregationType.TABLE):
            base += ["phase", "total_cost", "amount_received", "payment_percent", "status"]
        return list(dict.fromkeys(base))   # deduplicate, preserve order


# Module-level singleton
_parser: QueryParser | None = None


def get_query_parser() -> QueryParser:
    global _parser
    if _parser is None:
        _parser = QueryParser()
    return _parser
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

from app.models.models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType, OutputFormat,
)
from app.services.llm_client import get_llm_client
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic system prompt  (unchanged from v2 — provider-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

def _build_system_prompt(schema: dict[str, dict]) -> str:
    schema_lines: list[str] = []
    for tab_name, columns in schema.items():
        schema_lines.append(f'\nSheet tab: "{tab_name}"')
        for col_name, meta in columns.items():
            col_type = meta.get("type", "text")
            if col_type == "empty":
                schema_lines.append(
                    f'  - "{col_name}" (empty column | no data available yet)'
                )
            if col_type == "numeric":
                mn         = meta.get("min")
                mx         = meta.get("max")
                scale_hint = meta.get("scale_hint", "")
                range_str  = f"{mn} to {mx}" if mn is not None else "unknown range"
                schema_lines.append(
                    f'  - "{col_name}" (numeric | {scale_hint} | actual range: {range_str})'
                )
            else:
                samples    = meta.get("samples", [])
                sample_str = ", ".join(f'"{v}"' for v in samples) if samples else "no data"
                schema_lines.append(
                    f'  - "{col_name}" (text | sample values: {sample_str})'
                )

    schema_block = "\n".join(schema_lines)
    tab_list     = ", ".join(f'"{t}"' for t in schema.keys())

    return f"""You are a STRICT query parser for a business data tracking system backed by Google Sheets.

Your ONLY job is to convert a natural language question into a structured JSON query.
You do NOT answer questions.
You do NOT invent data.
You do NOT guess missing values

═══════════════════════════════════════════════════════
LIVE DATABASE SCHEMA (auto-generated from the spreadsheet)
═══════════════════════════════════════════════════════
{schema_block}

Available sheet tabs: {tab_list}
═══════════════════════════════════════════════════════

FIELD NAME RULES:
- Use the EXACT column name as shown (case-sensitive, spaces included).
- Use the EXACT tab name.
- NEVER invent column names or tab names.

FILTER OPERATORS: eq, neq, gt, gte, lt, lte, contains, not_contains, in, not_in  
AGGREGATION TYPES: list, count, sum, average, percentage, min, max  
OUTPUT FORMATS: single_value, list, table, summary  

JSON OUTPUT SCHEMA — return ONLY valid JSON:
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

═══════════════════════════════════════════════════════
STRICT RULES (DO NOT VIOLATE)
═══════════════════════════════════════════════════════

1. OUTPUT:
   - Return ONLY valid JSON.
   - No explanations, no markdown, no extra text.

2. CONFIDENCE:
   - Reflect how certain the mapping is (0.0–1.0).

3. SHEET SELECTION:
   - Use exact tab name if clear.
   - Otherwise set sheet_tab = null and ask for clarification.

4. NUMERIC VALUES:
   - Must be numeric types (not strings).

5. INTENT → AGGREGATION:
   - "how many" → count
   - "total / sum" → sum (set target_field)
   - "what percent" → percentage (set numerator_field + denominator_field)
   - "list / show" → list

6. COLUMN USAGE:
   - ONLY use columns present in schema.
   - NEVER map a concept to an unrelated column.
   - If no clear column exists → clarification_needed = true.

7. EMPTY COLUMNS (CRITICAL):
   - Columns marked as EMPTY have NO DATA.
   - YOU MUST NEVER:
     • use them in filters
     • use them in aggregation
     • assign any value to them
   - If user query depends on such a column:
     → clarification_needed = true
     → explain that the column has no data

8.  NO VALUE INVENTION (CRITICAL):
   - NEVER generate values like:
     "No", "Yes", "N/A", "None", "0"
   - UNLESS that exact value appears in the sample values.

9.  TEXT VALUE STRICT MATCHING:
   - You MUST ONLY use values from the provided sample values.
   - Match EXACT spelling and casing.
   - If the value is not in samples:
     → DO NOT GUESS
     → clarification_needed = true

10. NO DEFAULT ASSUMPTIONS:
    - If a column has no samples or unclear meaning:
      → DO NOT assume values
      → DO NOT create filters

11. DECIMAL RATIO HANDLING:
    - If column scale is 0–1:
      "50%" → 0.50 (NOT 50)

12. AMBIGUITY HANDLING:
    - If multiple interpretations exist:
      → clarification_needed = true
      → ask a clear question

13. FAILURE MODE (VERY IMPORTANT):
    - When unsure:
      DO NOT GUESS
      DO NOT INVENT
      ALWAYS ASK FOR CLARIFICATION

14. ZERO-DATA INTERPRETATION (IMPORTANT):
    If a user asks for absence of something (e.g. "no court cases",
    "without loans", "no complaints") AND the relevant column exists
    but is EMPTY:

    - DO NOT create a filter
    - DO NOT assign values like "No"

    Instead:
    - Return with NO filters
    - Keep intent and aggregation correct
    - Optionally lower confidence slightly

    Reason: Empty column means no usable data, not a valid filter condition.

═══════════════════════════════════════════════════════
BEHAVIOR SUMMARY
═══════════════════════════════════════════════════════

- Prefer returning NO FILTER over a WRONG FILTER
- Prefer clarification over guessing
- Never fabricate values
- Empty columns are unusable

"""


def _build_header_row_detection_prompt(rows_preview: list[list[str]]) -> str:
    """
    Build a prompt that lets Claude decide which row is the header
    by seeing ALL candidate rows at once — not just one row's values.

    Why multi-row context matters:
      - A header row is only meaningful relative to what comes after it.
      - Seeing Row 0 = ["Name", "Amount", "Date"] vs Row 1 = ["Alice", "5000", "2024-01-01"]
        is far more reliable than inspecting one row in isolation.

    Args:
        rows_preview: First N rows of the sheet, each row as a list of
                      raw cell strings (integer-indexed, no header yet).

    Returns:
        Prompt string to send to the LLM.
    """
    if not rows_preview:
        raise ValueError("rows_preview must contain at least one row")

    # Format each row for readability
    rows_block = "\n".join(
        f'  Row {i}: {[str(cell).strip() for cell in row]}'
        for i, row in enumerate(rows_preview)
    )

    return f"""You are analysing raw spreadsheet data that was loaded WITHOUT any header row.

Below are the first rows of the sheet (0-indexed). Each row is shown as a Python list of raw cell strings:

{rows_block}

Your task: identify which row index (0-based) is the HEADER row — the row that contains
column names rather than actual data values.

SIGNALS that a row is a HEADER (high weight):
  • Cells are short descriptive labels  (e.g. "Customer Name", "Invoice Date", "Total Amount")
  • Cells contain words, not numbers or dates
  • No currency symbols, percentages, or numeric formatting
  • Values are unique within the row (no repeated labels)
  • The rows that follow it look like data records (numbers, dates, names, IDs)

SIGNALS that a row is DATA (not a header):
  • Cells contain numeric values, dates, or currency amounts
  • Cells contain proper nouns, IDs, or codes  (e.g. "A-302", "INV-001", "John Smith")
  • Cells repeat patterns seen in other data rows

EDGE CASES:
  • If Row 0 contains headers but Row 1 is a sub-header (e.g. units row), return 0.
  • If the very first row is clearly data and no header row exists, return 0.
  • If multiple rows could be headers, return the LOWEST index.

Return ONLY a valid JSON object — no explanation, no markdown:
{{
    "header_row_index": <integer — the 0-based row index of the header>,
    "confidence": <float between 0.0 and 1.0>,
    "reason": "<one short sentence explaining your choice>"
}}"""

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
            if agg in (AggregationType.LIST,"list",):
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
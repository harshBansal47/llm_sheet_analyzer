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
import re
import time

from app.models.models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType, OutputFormat,
)
from app.services.llm_client import get_llm_client
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _build_column_line(col_name: str, meta: dict) -> str | None:
    """Convert a single column's schema dict into a prompt line."""
    t = meta.get("type", "unknown")

    # Skip structural ghost columns (unnamed cells → became "_2", "_3", etc.)
    if re.match(r"^_\d+$", col_name):
        return None

    if t == "empty":
        return f'  - "{col_name}" (empty column | NO DATA — never use in filters or aggregation)'

    if t == "identifier":
        mn      = meta.get("min")
        mx      = meta.get("max")
        samples = meta.get("samples", [])
        detail  = (
            f"range: {mn}–{mx}"
            if mn is not None
            else ", ".join(f'"{s}"' for s in samples[:5])
        )
        return f'  - "{col_name}" (identifier | {detail} | use eq or in only | ALWAYS trust user-provided values, never validate existence)'

    if t == "categorical":
        vals = ", ".join(f'"{v}"' for v in meta.get("values", []))
        return f'  - "{col_name}" (categorical | allowed values: {vals} | use exact match or in)'

    if t == "boolean":
        vals = ", ".join(f'"{v}"' for v in meta.get("values", []))
        return f'  - "{col_name}" (boolean | values: {vals} | use exact match)'

    if t == "grade":
        vals = " > ".join(meta.get("values", []))
        return f'  - "{col_name}" (grade | ordinal best→worst: {vals} | use exact match or in)'

    if t == "date":
        samples = ", ".join(f'"{s}"' for s in meta.get("samples", [])[:4])
        return f'  - "{col_name}" (date | samples: {samples} | use date comparison operators: gt, lt, gte, lte)'

    if t == "currency":
        mn = meta.get("min")
        mx = meta.get("max")
        return f'  - "{col_name}" (currency ₹ | range: {mn}–{mx} | use numeric operators, do not use contains)'

    if t == "percentage":
        mn = meta.get("min")
        mx = meta.get("max")
        return f'  - "{col_name}" (percentage | range: {mn}%–{mx}% | values are already in % scale, e.g. 40 means 40%)'

    if t == "numeric":
        mn           = meta.get("min")
        mx           = meta.get("max")
        all_integers = meta.get("all_integers", False)
        kind         = "integer" if all_integers else "decimal"
        return f'  - "{col_name}" (numeric {kind} | range: {mn}–{mx} | use numeric operators)'

    if t == "phone":
        return f'  - "{col_name}" (phone number | treat as string | never aggregate or use numeric operators)'

    if t == "email":
        return f'  - "{col_name}" (email address | use eq or contains only)'

    if t == "free_text":
        samples = ", ".join(f'"{s}"' for s in meta.get("samples", [])[:4])
        return f'  - "{col_name}" (free text | samples: {samples} | use contains or not_contains | never ask user to confirm exact spelling)'

    # fallback — unknown type, expose samples if present
    samples    = meta.get("samples", [])
    sample_str = ", ".join(f'"{v}"' for v in samples) if samples else "no data"
    return f'  - "{col_name}" (text | sample values: {sample_str})'


def _build_system_prompt(schema: dict[str, dict]) -> str:
    schema_lines: list[str] = []

    for tab_name, columns in schema.items():
        schema_lines.append(f'\nSheet tab: "{tab_name}"')
        for col_name, meta in columns.items():
            line = _build_column_line(col_name, meta)
            if line is not None:
                schema_lines.append(line)

    schema_block = "\n".join(schema_lines)
    tab_list     = ", ".join(f'"{t}"' for t in schema.keys())

    return f"""You are a STRICT query parser for a business data tracking system backed by Google Sheets.
Your ONLY job is to convert a natural language question into a structured JSON query.
You do NOT answer questions.
You do NOT invent data.
You do NOT guess missing values.

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
  "intent":                string,
  "sheet_tab":             string | null,
  "filters":               [{{"field": string, "operator": string, "value": any}}],
  "aggregation":           string,
  "display_fields":        [string],
  "target_field":          string | null,
  "numerator_field":       string | null,
  "denominator_field":     string | null,
  "output_format":         string,
  "confidence":            float (0.0–1.0),
  "clarification_needed":  bool,
  "clarification_message": string
}}

═══════════════════════════════════════════════════════
COLUMN TYPE RULES
═══════════════════════════════════════════════════════

CATEGORICAL columns:
  - Filter values MUST come from the listed "allowed values" exactly.
  - Match exact spelling and casing.
  - If the value the user mentions is not in the allowed list → clarification_needed = true.

BOOLEAN columns:
  - Only use the exact values listed. Never invent "Yes"/"No"/"True"/"False" unless listed.

GRADE columns:
  - Ordinal type. Use eq or in for exact grade. Use gt/lt only if comparing rank order.

DATE columns:
  - Use gt, gte, lt, lte for range queries.
  - Use eq only for exact date match.
  - Never use contains on a date column.

CURRENCY columns:
  - Use numeric operators only (gt, gte, lt, lte, eq).
  - Never use contains.
  - Values are raw numbers (e.g. 3430000, not "34,30,000").

PERCENTAGE columns:
  - Values are already on a 0–100 scale.
  - "more than 50%" → value: 50 with operator: gt.
  - Never convert to 0–1 decimal.

PHONE / EMAIL columns:
  - Treat as strings. Use eq or contains only.
  - Never aggregate, sum, or apply numeric operators.

NUMERIC columns:
  - Use numeric operators. Never use contains.

FREE TEXT columns:
  - Use contains or not_contains for partial matches.
  - Use eq only when the user provides an exact value.

IDENTIFIER columns:
  - Use eq or in only. Never use contains or numeric operators.
  - ALWAYS trust any identifier value the user provides (Apt No., SN, ID, etc.).
  - NEVER tell the user an identifier "does not exist" — build the query and let the engine handle it.

EMPTY columns:
  - Have absolutely NO data.
  - NEVER use in filters, aggregations, or display_fields.
  - If the query depends on an empty column → clarification_needed = true.

═══════════════════════════════════════════════════════
NAME & FREE TEXT MATCHING
═══════════════════════════════════════════════════════

- ALWAYS use "contains" operator for name-based searches. NEVER ask the user for exact spelling.
- Treat names as case-insensitive partial matches — the execution engine handles case folding.
- If a name query could match multiple records, set aggregation to "list" and let the engine return all matches.
- For numeric fields on multiple matched records (e.g. "sum received for Vaibhav"), set aggregation to "sum".
- NEVER say "Name matching is case-sensitive" or ask the user to confirm spelling.

═══════════════════════════════════════════════════════
NO MEMORY — STATELESS OPERATION
═══════════════════════════════════════════════════════

- You have NO memory of previous messages. Every query is fully independent.
- If the query contains pronouns (his, her, their, its, the same, above) with NO
  name, ID, or Apt No. present in the CURRENT message:
    → clarification_needed = true
    → clarification_message MUST say exactly:
      "I don't have memory of previous messages. Please include the name or
       apartment number in this message. Example: '[Name/Apt No.] cost and received'"
- NEVER reference or assume context from a prior turn.

═══════════════════════════════════════════════════════
VALUE EXISTENCE VALIDATION — STRICTLY FORBIDDEN
═══════════════════════════════════════════════════════

- You do NOT have access to row-level data — only the schema (column names and types).
- NEVER tell the user that a value "does not exist", "is not in the database",
  or "could not be found". That is the execution engine's job, not yours.
- Always build the query with whatever value the user provides.
- This applies to ALL column types: identifiers, names, apartment numbers, SNs, etc.

═══════════════════════════════════════════════════════
STRICT RULES (DO NOT VIOLATE)
═══════════════════════════════════════════════════════

1. OUTPUT:
   - Return ONLY valid JSON. No markdown, no explanation, no extra text.

2. CONFIDENCE:
   - Reflect certainty of mapping (0.0–1.0). Lower if ambiguous or partial match.

3. SHEET SELECTION:
   - Use exact tab name when unambiguous. Otherwise sheet_tab = null + clarification.

4. INTENT → AGGREGATION MAPPING:
   - "how many"             → count
   - "total" / "sum of"     → sum  (set target_field)
   - "what percent"         → percentage  (set numerator_field + denominator_field)
   - "list" / "show" / "who"→ list
   - "average"              → average  (set target_field)

5. VALUE INVENTION — ABSOLUTELY FORBIDDEN:
   - Never generate filter values not present in the schema's allowed/sample values.
   - This includes "No", "Yes", "N/A", "None", "0", "Unknown".

6. AMBIGUITY:
   - If multiple columns could match the user's intent → clarification_needed = true.
   - Ask a short, clear question.

7. ZERO-DATA QUERIES:
   - If the user asks about absence (e.g. "no loan", "no court case") and the
     relevant column is EMPTY → do NOT create a filter. Return no filters, correct
     aggregation, and slightly lower confidence.

8. FAILURE MODE:
   - When unsure → DO NOT GUESS. Set clarification_needed = true and ask.

═══════════════════════════════════════════════════════
BEHAVIOR SUMMARY
═══════════════════════════════════════════════════════
- A wrong filter is worse than no filter.
- Clarification beats guessing every time.
- Empty columns do not exist for query purposes.
- Column types dictate which operators are legal.
- You build queries. You never validate data existence.
- You have no memory. Every message stands alone.
- Names are always partial contains matches. Never ask for exact spelling.
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
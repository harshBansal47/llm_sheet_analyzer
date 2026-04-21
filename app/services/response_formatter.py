"""
services/response_formatter.py  –  QueryResult → Telegram-optimised Message

Output structure for row results:
  📋 <N> records found

  #1 · SN: 45 · M/S COGERS TRADEX PVT LTD · L-011
  #2 · SN: 46 · Urmila Gupta · L-031
  ...
  _...and N more_

  🎯 Filter1 | Filter2
  📅 Data as of <timestamp>

Scalar output:
  📊 <label>
     → <value>
     (based on N records)
  📅 Data as of <timestamp>

Design rules:
  - SN is ALWAYS injected at position 0 — client navigation anchor to find rows in sheet
  - One line per record (scannable in Telegram)
  - Values auto-formatted (currency ₹ Indian units, %, compact numbers)
  - No hardcoded field names — schema-agnostic
  - MAX 15 rows; remainder shown as count
  - Filters shown as compact pills at the bottom
  - Name fields get 30 chars (not 22) — primary identifier deserves space
  - Tower redundancy avoided when Apt No. already encodes it
"""
from __future__ import annotations
import math
from app.models.models import QueryResult
from app.services.query_engine import QueryEngine
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_ROWS       = 15   # Maximum record lines in one message
MAX_FIELDS     = 6    # Max fields shown per record line (including SN)
MAX_VAL_LEN    = 22   # Truncate generic string values to this length
MAX_NAME_LEN   = 30   # Name/address fields get more space — primary identifier
MAX_LABEL_LEN  = 14   # Truncate long field labels to this length

# Fields that are purely internal — never shown to the user
_INTERNAL_FIELDS = {"_source_tab"}

# Column names treated as row identifiers (SN pinned first always)
_SN_NAMES = {"SN", "S.NO", "SR", "SR.NO", "ID"}

# Operator symbols for filter summary line
_OP_SYMBOL: dict[str, str] = {
    "eq":           "=",
    "neq":          "≠",
    "gt":           ">",
    "gte":          "≥",
    "lt":           "<",
    "lte":          "≤",
    "contains":     "~",
    "not_contains": "!~",
    "in":           "∈",
    "not_in":       "∉",
}


class ResponseFormatter:

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry points
    # ──────────────────────────────────────────────────────────────────────────

    def format_telegram(self, result: QueryResult) -> str:
        if not result.success:
            return f"❌ {result.error_message}"
        if result.scalar_value is not None:
            return self._format_scalar(result)
        return self._format_rows(result)

    def format_whatsapp(self, result: QueryResult) -> str:
        """Plain-text version — same structure, no Markdown."""
        return (
            self.format_telegram(result)
            .replace("*", "")
            .replace("_", "")
            .replace("`", "")
        )

    def format_error(self, error_message: str) -> str:
        return f"❌ *Error*\n\n{error_message}"

    # ──────────────────────────────────────────────────────────────────────────
    # Scalar result  (count / sum / average / min / max / percentage)
    # ──────────────────────────────────────────────────────────────────────────

    def _format_scalar(self, result: QueryResult) -> str:
        val   = QueryEngine.format_scalar(result.scalar_value)
        label = result.scalar_label or "Result"
        n     = result.total_rows_matched
        ts    = result.sheet_last_refreshed

        lines = [
            f"📊 *{label}*",
            "",
            f"  → `{val}`",
            f"  _(based on {n} matching record{'s' if n != 1 else ''})_",
        ]

        filter_line = self._build_filter_summary(result)
        if filter_line:
            lines += ["", f"🎯 {filter_line}"]

        lines += ["", f"📅 _Data as of {ts}_"]
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────────────────────
    # Row result  (list / table)
    # ──────────────────────────────────────────────────────────────────────────

    def _format_rows(self, result: QueryResult) -> str:
        rows = result.rows
        n    = result.total_rows_matched
        ts   = result.sheet_last_refreshed

        if n == 0:
            return f"🔍 *No records found.*\n\n📅 _Data as of {ts}_"

        header = f"📋 *{n} record{'s' if n != 1 else ''} found*"
        lines  = [header, ""]

        display_fields = self._resolve_display_fields(rows[0], result)

        for i, row in enumerate(rows, 1):
            lines.append(self._format_record_line(i, row, display_fields))

        filter_line = self._build_filter_summary(result)
        if filter_line:
            lines += ["", f"🎯 {filter_line}"]

        lines += ["", f"📅 _Data as of {ts}_"]
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────────────────────
    # Field selection
    # ──────────────────────────────────────────────────────────────────────────

    def _resolve_display_fields(
        self,
        sample_row: dict,
        result: QueryResult,
    ) -> list[str]:
        """
        Determine the ordered list of fields to show per record.

        Rules (in priority order):
          1. SN is ALWAYS injected at position 0 — non-negotiable navigation anchor.
             Without SN the client cannot locate the record in a 700-row sheet.
          2. If parser provided display_fields → use them after injecting SN.
          3. Fallback → natural column order capped at MAX_FIELDS.
        """
        all_cols = [k for k in sample_row if k not in _INTERNAL_FIELDS]

        # Find the SN column in this row's keys
        sn_col = next(
            (c for c in all_cols if c.strip().upper() in _SN_NAMES),
            None
        )

        # ── Case 1: Parser specified display_fields ───────────────────────────
        if result.structured_query and result.structured_query.display_fields:
            requested = [
                f for f in result.structured_query.display_fields
                if f in sample_row and f not in _INTERNAL_FIELDS
            ]
            if requested:
                # Force SN at front even if parser didn't ask for it
                if sn_col and sn_col not in requested:
                    requested = [sn_col] + requested
                return requested[:MAX_FIELDS]

        # ── Case 2: Fallback — natural column order with SN pinned ────────────
        if sn_col:
            ordered = [sn_col] + [c for c in all_cols if c != sn_col]
            return ordered[:MAX_FIELDS]

        return all_cols[:MAX_FIELDS]

    # ──────────────────────────────────────────────────────────────────────────
    # Single record line
    # ──────────────────────────────────────────────────────────────────────────

    def _format_record_line(
        self,
        idx: int,
        row: dict,
        fields: list[str],
    ) -> str:
        """
        Produces a compact single line:
          *#1* · SN: 45 · Urmila Gupta · H-012 · Rcvd%: 51.8%

        SN is always shown with its label so the client can cross-reference the sheet.
        """
        parts: list[str] = [f"*#{idx}*"]

        # Multi-tab provenance
        if "_source_tab" in row:
            parts.append(f"_[{row['_source_tab']}]_")

        for field in fields:
            val = row.get(field)
            if val in ("", None, "nan", "None"):
                continue
            parts.append(self._fmt_field_value(field, val))

        return " · ".join(parts)

    # ──────────────────────────────────────────────────────────────────────────
    # Value formatting
    # ──────────────────────────────────────────────────────────────────────────

    def _fmt_field_value(self, field: str, value) -> str:
        """
        Format one field-value pair as a compact token.

        Examples:
          SN=45             → "SN: 45"            (always labelled — navigation anchor)
          Name=Urmila Gupta → "Urmila Gupta"       (label dropped — value is self-evident)
          Apt No.=H-012     → "H-012"              (short code, no label)
          Balance=6815846   → "Bal: ₹68.2L"
          Received%=51.83   → "Rcvd % age: 51.8%"
          Tower=H           → "H"                  (single-char, no label)
        """
        field_lower = field.lower()
        label       = self._short_label(field)
        is_sn       = field.strip().upper() in _SN_NAMES

        # ── Numeric formatting ────────────────────────────────────────────────
        try:
            fval = float(value)

            # SN — always labelled so client knows this is the sheet row number
            if is_sn:
                return f"SN: {int(fval)}"

            # Percentage column
            if "%" in field or any(w in field_lower for w in ("percent", "pct", "received")):
                if 0.0 <= fval <= 1.0:
                    return f"{label}: {fval:.1f}%"
                return f"{label}: {fval:.1f}"

            # Currency column
            if any(w in field_lower for w in (
                "price", "amount", "amt", "value", "balance", "bal",
                "demanded", "paid", "sale",
            )):
                return f"{label}: {self._fmt_inr(fval)}"

            # Small integer (phase, count, etc.)
            if fval == int(fval):
                ival = int(fval)
                return f"{label}: {ival}" if ival <= 9999 else f"{label}: {ival:,}"

            return f"{label}: {fval:.2f}"

        except (ValueError, TypeError):
            pass

        # ── String formatting ─────────────────────────────────────────────────
        str_val = str(value).strip()

        # Single-char or very short categoricals (Tower: H, Type: NEW) — no label
        if len(str_val) <= 4:
            return str_val

        # Name / address fields — primary human identifier, deserves more space
        if any(w in field_lower for w in ("name", "address", "addr")):
            if len(str_val) > MAX_NAME_LEN:
                str_val = str_val[:MAX_NAME_LEN - 1] + "…"
            return str_val   # label dropped — value is self-describing

        # Generic string — truncate if too long
        if len(str_val) > MAX_VAL_LEN:
            str_val = str_val[:MAX_VAL_LEN - 1] + "…"

        return f"{label}: {str_val}"

    # ──────────────────────────────────────────────────────────────────────────
    # Filter summary line
    # ──────────────────────────────────────────────────────────────────────────

    def _build_filter_summary(self, result: QueryResult) -> str:
        """
        Compact filter pills at the bottom:
          Phase = 1  |  Rcvd % age > 50  |  Type = NEW
        """
        if not result.structured_query or not result.structured_query.filters:
            return ""

        pills: list[str] = []
        for f in result.structured_query.filters[:6]:
            sym   = _OP_SYMBOL.get(f.operator, f.operator)
            label = self._short_label(f.field)
            val   = f.value

            if isinstance(val, list):
                val_str = (
                    f"[{', '.join(str(v) for v in val[:3])}"
                    f"{'…' if len(val) > 3 else ''}]"
                )
            else:
                try:
                    fv      = float(val)
                    val_str = self._fmt_inr(fv) if fv >= 1000 else str(val)
                except (TypeError, ValueError):
                    val_str = str(val)

            pills.append(f"{label} {sym} {val_str}")

        return "  |  ".join(pills)

    # ──────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _short_label(field: str) -> str:
        """
        Compact display label from a column name.

        Examples:
          "Received Amt With Tax"                → "Rcvd Amt"
          "Balance Amt"                          → "Bal Amt"
          "Total Sale Value with Tax incl. IFMS" → "Total Sale…"
          "Received % age"                       → "Rcvd % age"
        """
        substitutions: dict[str, str] = {
            "received":  "Rcvd",
            "balance":   "Bal",
            "amount":    "Amt",
            "number":    "No",
            "payment":   "Pymnt",
            "original":  "Orig",
            "with":      "",
            "including": "",
            "tax":       "",
            "and":       "",
        }
        tokens       = field.replace("\n", " ").split()
        short_tokens = []
        for tok in tokens:
            replacement = substitutions.get(tok.lower())
            if replacement is None:
                short_tokens.append(tok)
            elif replacement:
                short_tokens.append(replacement)
            # empty string replacement → word dropped

        label = " ".join(short_tokens).strip()
        if len(label) > MAX_LABEL_LEN:
            label = label[:MAX_LABEL_LEN - 1] + "…"
        return label or field[:MAX_LABEL_LEN]

    @staticmethod
    def _fmt_inr(val: float) -> str:
        """
        Indian currency compact format:
          6,815,846 → ₹68.2L
          500,000   → ₹5.0L
          45,000    → ₹45.0k
          1,500     → ₹1,500
        """
        if val >= 10_000_000:
            return f"₹{val / 10_000_000:.1f}Cr"
        if val >= 100_000:
            return f"₹{val / 100_000:.1f}L"
        if val >= 1_000:
            return f"₹{val / 1_000:.1f}k"
        return f"₹{int(val):,}"


# ─────────────────────────────────────────────────────────────────────────────
_formatter: ResponseFormatter | None = None


def get_formatter() -> ResponseFormatter:
    global _formatter
    if _formatter is None:
        _formatter = ResponseFormatter()
    return _formatter
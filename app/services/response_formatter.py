"""
services/response_formatter.py  –  QueryResult → Telegram-optimised Message

Output structure for row results:
  📋 <N> records — <short intent summary>

  #1 · SN-001 · Name · Field1: val · Field2: val
  #2 · SN-002 · Name · Field1: val · Field2: val
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
  - One line per record (scannable in Telegram)
  - SN shown first so users can reference rows easily
  - Values auto-formatted (currency ₹, %, compact numbers)
  - No hardcoded field names — schema-agnostic
  - MAX 15 rows; remainder shown as count
  - Filters shown as compact pills at the bottom
"""
from __future__ import annotations
import math
from app.models.models import QueryResult
from app.services.query_engine import QueryEngine
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_ROWS      = 15   # Maximum record lines in one message
MAX_FIELDS    = 6    # Max fields shown per record line (excluding SN)
MAX_VAL_LEN   = 22   # Truncate long string values to this length
MAX_LABEL_LEN = 14   # Truncate long field names to this length

# Fields that are purely internal — never shown to the user
_INTERNAL_FIELDS = {"_source_tab"}

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
        return self.format_telegram(result)\
            .replace("*", "").replace("_", "").replace("`", "")

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

        # Filters summary
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

        # ── Summary header line ───────────────────────────────────────────────
        header = f"📋 *{n} record{'s' if n != 1 else ''} found*"
        lines  = [header, ""]

        # ── Decide which fields to display ────────────────────────────────────
        display_fields = self._resolve_display_fields(rows[0], result)

        # ── One line per record ───────────────────────────────────────────────
        for i, row in enumerate(rows[:MAX_ROWS], 1):
            lines.append(self._format_record_line(i, row, display_fields))

        # ── "...and N more" tail ──────────────────────────────────────────────
        if n > MAX_ROWS:
            remaining = n - MAX_ROWS
            lines.append(
                f"\n_...and {remaining} more record{'s' if remaining != 1 else ''}_"
            )

        # ── Filter summary ────────────────────────────────────────────────────
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

        Priority:
          1. Parser-provided display_fields (already curated)
          2. All non-internal columns in schema order (capped at MAX_FIELDS)

        SN (or the first column) is always shown first when present.
        """
        all_cols = [k for k in sample_row if k not in _INTERNAL_FIELDS]

        # Parser specified fields explicitly
        if result.structured_query and result.structured_query.display_fields:
            requested = [
                f for f in result.structured_query.display_fields
                if f in sample_row and f not in _INTERNAL_FIELDS
            ]
            if requested:
                return self._pin_sn_first(requested, all_cols)[:MAX_FIELDS]

        # Fallback: natural column order
        return self._pin_sn_first(all_cols, all_cols)[:MAX_FIELDS]

    @staticmethod
    def _pin_sn_first(fields: list[str], all_cols: list[str]) -> list[str]:
        """
        Move 'SN' (or the first column of the sheet) to position 0
        so users always have a reference number.
        """
        # Prefer explicit SN column
        sn_candidates = [f for f in all_cols if f.strip().upper() in ("SN", "S.NO", "SR", "SR.NO", "ID")]
        if sn_candidates:
            sn = sn_candidates[0]
            return [sn] + [f for f in fields if f != sn]

        # No SN column — keep order as-is (first col acts as identifier)
        return fields

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
          #3 · 15 · Urmila Gupta · Bal: ₹24.3L · Rcvd: 51.8%

        - idx      : display position (1-based)
        - row      : the data dict
        - fields   : ordered list of fields to render
        """
        parts: list[str] = [f"*#{idx}*"]

        # Source tab provenance (multi-tab queries)
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
        Auto-detects type from value and field name — no hardcoding.

        Examples:
          SN=1          → "1"               (identifier, no label)
          Name=...      → "Urmila Gupta"    (first meaningful field, no label)
          Balance=6815846 → "Bal: ₹6.8L"
          Received%=51.83 → "Rcvd%: 51.8%"
          Tower=H       → "H"              (short categoricals, no label)
        """
        field_lower = field.lower()
        label       = self._short_label(field)

        # ── Numeric formatting ────────────────────────────────────────────────
        try:
            fval = float(value)

            # Percentage column
            if "%" in field or any(w in field_lower for w in ("percent", "pct", "rate", "received")):
                if 0.0 <= fval <= 1.0:
                    return f"{label}: {fval * 100:.1f}%"
                return f"{label}: {fval:.1f}%"

            # Currency column
            if any(w in field_lower for w in (
                "price", "amount", "amt", "value", "balance", "bal",
                "demanded", "paid", "sale", "basic rate",
            )):
                return f"{label}: {self._fmt_inr(fval)}"

            # Plain integer — if small, show as-is (SN, Phase, etc.)
            if fval == int(fval):
                ival = int(fval)
                if ival <= 9999:
                    return str(ival) if label.upper() in ("SN", "SR", "ID") else f"{label}: {ival}"
                return f"{label}: {ival:,}"

            return f"{label}: {fval:.2f}"

        except (ValueError, TypeError):
            pass

        # ── String formatting ─────────────────────────────────────────────────
        str_val = str(value).strip()

        # Very short values (Tower: H, Phase: 2) — skip label for readability
        if len(str_val) <= 4:
            return str_val

        # Truncate long strings
        if len(str_val) > MAX_VAL_LEN:
            str_val = str_val[:MAX_VAL_LEN - 1] + "…"

        # Name-like fields — show value only, no label
        if any(w in field_lower for w in ("name", "address", "addr")):
            return str_val

        return f"{label}: {str_val}"

    # ──────────────────────────────────────────────────────────────────────────
    # Filter summary line
    # ──────────────────────────────────────────────────────────────────────────

    def _build_filter_summary(self, result: QueryResult) -> str:
        """
        Produces a compact filter pill line:
          Tower = H  |  Balance ≥ 500000  |  Type ∈ [NEW, OLD]
        """
        if not result.structured_query or not result.structured_query.filters:
            return ""

        pills: list[str] = []
        for f in result.structured_query.filters[:6]:
            sym   = _OP_SYMBOL.get(f.operator, f.operator)
            label = self._short_label(f.field)
            val   = f.value

            if isinstance(val, list):
                val_str = f"[{', '.join(str(v) for v in val[:3])}{'…' if len(val) > 3 else ''}]"
            else:
                # Format numeric filter values
                try:
                    fv = float(val)
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
        Produce a compact display label from a column name.
        Strips common noise words, truncates to MAX_LABEL_LEN.

        Examples:
          "Received Amt With Tax"             → "Rcvd Amt"
          "Balance Amt"                       → "Bal Amt"
          "Basic Sale Price"                  → "Sale Price"
          "Total Sale Value with Tax..."      → "Total Sale…"
        """
        # Common substitutions for long words
        substitutions = {
            "received":  "Rcvd",
            "balance":   "Bal",
            "amount":    "Amt",
            "number":    "No",
            "payment":   "Pymnt",
            "original":  "Orig",
            "total":     "Total",
            "with":      "",
            "including": "",
            "tax":       "",
            "and":       "",
        }
        tokens = field.replace("\n", " ").split()
        short_tokens = []
        for tok in tokens:
            replacement = substitutions.get(tok.lower())
            if replacement is None:
                short_tokens.append(tok)
            elif replacement:
                short_tokens.append(replacement)
            # empty replacement → skip the word

        label = " ".join(short_tokens).strip()
        if len(label) > MAX_LABEL_LEN:
            label = label[:MAX_LABEL_LEN - 1] + "…"
        return label or field[:MAX_LABEL_LEN]

    @staticmethod
    def _fmt_inr(val: float) -> str:
        """
        Indian currency compact format:
          6815846 → ₹68.2L
          500000  → ₹5.0L
          45000   → ₹45.0k
          1500    → ₹1,500
        """
        if val >= 10_000_000:   # 1 Cr+
            return f"₹{val / 10_000_000:.1f}Cr"
        if val >= 100_000:      # 1 L+
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
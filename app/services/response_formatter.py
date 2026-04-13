"""
services/response_formatter.py  –  QueryResult → Human-readable message

Formats the deterministic query result into clean, readable text
suitable for Telegram (Markdown) or WhatsApp (plain text).

NO data is generated here. Only formatting of existing QueryResult.
"""
from __future__ import annotations
from app.models.models import QueryResult, AggregationType, OutputFormat
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_ROWS_IN_MESSAGE = 20    # Telegram has a 4096 char limit


class ResponseFormatter:

    def format_telegram(self, result: QueryResult) -> str:
        """Return Markdown-formatted string for Telegram."""
        return self._format(result, use_markdown=True)

    def format_whatsapp(self, result: QueryResult) -> str:
        """Return plain text for WhatsApp."""
        return self._format(result, use_markdown=False)

    def format_error(self, error_message: str) -> str:
        return f"❌ *Error*\n\n{error_message}"

    # ──────────────────────────────────────────────────────────────────────────

    def _format(self, result: QueryResult, use_markdown: bool) -> str:
        if not result.success:
            sym = "❌"
            msg = f"{sym} {result.error_message}"
            return msg

        query = result.structured_query
        agg   = query.aggregation if query else "list"

        # ── Scalar result ────────────────────────────────────────────────────
        if result.scalar_value is not None:
            return self._format_scalar(result, use_markdown)

        # ── Row-level result ─────────────────────────────────────────────────
        return self._format_rows(result, use_markdown)

    def _format_scalar(self, result: QueryResult, md: bool) -> str:
        val   = result.scalar_value
        label = result.scalar_label
        n     = result.total_rows_matched
        ts    = result.sheet_last_refreshed

        b = "**" if md else ""
        lines = [
            f"📊 {b}{label}{b}",
            "",
            f"  → {b}{val}{b}",
            f"  (Based on {n} matching record{'s' if n != 1 else ''})",
            "",
            f"_Data as of {ts}_" if md else f"Data as of {ts}",
        ]
        return "\n".join(lines)

    def _format_rows(self, result: QueryResult, md: bool) -> str:
        rows = result.rows
        n    = result.total_rows_matched
        ts   = result.sheet_last_refreshed
        b    = "**" if md else ""

        if n == 0:
            return f"🔍 No records found matching your query.\n\n_Data as of {ts}_" if md \
                   else f"No records found matching your query.\nData as of {ts}"

        # Header
        header = f"📋 {b}{n} record{'s' if n != 1 else ''} found{b}"

        # Truncate if too many rows
        display_rows = rows[:MAX_ROWS_IN_MESSAGE]
        truncated    = n > MAX_ROWS_IN_MESSAGE

        # Build individual record blocks
        parts = [header, ""]
        for i, row in enumerate(display_rows, 1):
            parts.append(self._format_row(i, row, md))

        if truncated:
            parts.append(f"\n_... and {n - MAX_ROWS_IN_MESSAGE} more records_" if md
                         else f"\n... and {n - MAX_ROWS_IN_MESSAGE} more records")

        parts.append("")
        parts.append(f"_Data as of {ts}_" if md else f"Data as of {ts}")
        return "\n".join(parts)

    def _format_row(self, idx: int, row: dict, md: bool) -> str:
        b = "**" if md else ""
        # Show source tab provenance if present (multi-tab search result)
        tab_line = ""
        if "_source_tab" in row:
            tab_line = f"  _{row['_source_tab']}_\n" if md else f"  [{row['_source_tab']}]\n"

        lines = [f"{b}#{idx}{b}"]
        if tab_line:
            lines.append(tab_line.rstrip())

        for key, val in row.items():
            if key == "_source_tab":
                continue
            if val in ("", "nan", "None", None):
                continue
            label = key  # use real column name directly — no canonical remapping
            # Smart formatting for obviously numeric-looking columns
            try:
                fval = float(val)
                col_lower = key.lower()
                if "percent" in col_lower or "%" in col_lower:
                    lines.append(f"  • {label}: {fval:.1f}%")
                elif any(w in col_lower for w in ("cost", "amount", "price", "value", "paid", "received")):
                    lines.append(f"  • {label}: ₹{fval:,.0f}")
                else:
                    lines.append(f"  • {label}: {self._fmt_numeric(fval)}")
            except (ValueError, TypeError):
                lines.append(f"  • {label}: {val}")
        return "\n".join(lines)

    @staticmethod
    def _fmt_numeric(val: float) -> str:
        if val == int(val):
            return f"{int(val):,}"
        return f"{val:,.2f}"


# Singleton
_formatter: ResponseFormatter | None = None


def get_formatter() -> ResponseFormatter:
    global _formatter
    if _formatter is None:
        _formatter = ResponseFormatter()
    return _formatter
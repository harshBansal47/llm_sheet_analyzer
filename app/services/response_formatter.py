"""
services/response_formatter.py  –  QueryResult → Human-readable Message

Completely schema-agnostic formatter that works with ANY sheet structure.
No hardcoded field names or business logic.
"""
from __future__ import annotations
from app.models.models import QueryResult, AggregationType, OutputFormat
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_ROWS_IN_MESSAGE = 10  # Reduced for better readability
MAX_FIELDS_PER_ROW = 8    # Show only most relevant fields in compact mode


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

        # ── Scalar result ────────────────────────────────────────────────────
        if result.scalar_value is not None:
            return self._format_scalar(result, use_markdown)

        # ── Row-level result ─────────────────────────────────────────────────
        return self._format_rows_compact(result, use_markdown)

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

    def _format_rows_compact(self, result: QueryResult, md: bool) -> str:
        """Format rows in a clean, scannable format suitable for any schema."""
        rows = result.rows
        n = result.total_rows_matched
        ts = result.sheet_last_refreshed
        b = "**" if md else ""

        if n == 0:
            return f"🔍 No records found.\n\n_Data as of {ts}_" if md \
                   else f"No records found.\nData as of {ts}"

        # Determine which fields to show (most informative ones)
        field_priority = self._get_field_priority(rows[0]) if rows else []
        
        # Header
        parts = [f"📋 {b}{n} record{'s' if n != 1 else ''} found{b}", ""]
        
        # Show records in a more compact inline format
        display_rows = rows[:MAX_ROWS_IN_MESSAGE]
        
        for i, row in enumerate(display_rows, 1):
            parts.append(self._format_inline_row(i, row, field_priority, md))
        
        if n > MAX_ROWS_IN_MESSAGE:
            remaining = n - MAX_ROWS_IN_MESSAGE
            parts.append(f"\n_...and {remaining} more record{'s' if remaining != 1 else ''}_" if md 
                        else f"\n...and {remaining} more record{'s' if remaining != 1 else ''}")
        
        # Add filters summary (auto-detected from query)
        if result.structured_query and result.structured_query.filters:
            parts.append("")
            parts.append("─" * 15)
            parts.append("🎯 *Applied filters:*" if md else "🎯 Applied filters:")
            for filter_cond in result.structured_query.filters[:5]:  # Show top 5 filters
                parts.append(self._format_filter_line(filter_cond, md))
        
        parts.append("")
        parts.append(f"_📅 Data as of {ts}_" if md else f"📅 Data as of {ts}")
        
        return "\n".join(parts)

    def _get_field_priority(self, sample_row: dict) -> list:
        """
        Determine which fields to show first based on data characteristics.
        Completely automatic - no hardcoded field names.
        """
        fields = []
        
        for key, val in sample_row.items():
            if key.startswith('_'):  # Skip internal fields
                continue
            
            priority = 0
            key_lower = key.lower()
            
            # Heuristics for field importance (still schema-agnostic)
            if any(word in key_lower for word in ['id', 'name', 'title']):
                priority += 10
            if any(word in key_lower for word in ['amount', 'price', 'cost', 'value']):
                priority += 5
            if any(word in key_lower for word in ['date', 'time', 'year']):
                priority += 3
            
            # Numeric fields often contain key metrics
            try:
                float(val)
                priority += 2
            except (ValueError, TypeError):
                pass
            
            fields.append((key, priority))
        
        # Sort by priority, return top fields
        fields.sort(key=lambda x: x[1], reverse=True)
        return [f[0] for f in fields[:MAX_FIELDS_PER_ROW]]

    def _format_inline_row(self, idx: int, row: dict, priority_fields: list, md: bool) -> str:
        """
        Format a single row in a compact, inline style.
        Example: #1 | ID: 33 | Age: 59 | Income: ₹131k | Score: 80%
        """
        b = "**" if md else ""
        
        # Start with record number
        parts = [f"{b}#{idx}{b}"]
        
        # Add tab info if present
        if "_source_tab" in row:
            parts.append(f"_[{row['_source_tab']}]_" if md else f"[{row['_source_tab']}]")
        
        # Build inline field-value pairs
        field_values = []
        
        # First show priority fields
        for field in priority_fields:
            if field in row:
                val = row[field]
                if val and val not in ("", "nan", "None", None):
                    formatted = self._format_value_compact(field, val)
                    field_values.append(formatted)
        
        # If we have room, add other fields
        other_fields = [f for f in row.keys() if f not in priority_fields and not f.startswith('_')]
        for field in other_fields[:3]:  # Add at most 3 extra fields
            val = row[field]
            if val and val not in ("", "nan", "None", None):
                formatted = self._format_value_compact(field, val)
                field_values.append(formatted)
        
        # Join everything with separators
        if field_values:
            parts.append(" • " + " • ".join(field_values))
        
        return "".join(parts)

    def _format_value_compact(self, field: str, value) -> str:
        """
        Format a single field-value pair compactly.
        Auto-detects number formatting without hardcoding.
        """
        # Clean field name for display
        display_name = field.replace('_', ' ').title()
        if len(display_name) > 15:
            display_name = display_name[:12] + "..."
        
        try:
            # Try to format as number
            fval = float(value)
            
            # Intelligent number formatting
            if fval >= 1_000_000:
                formatted = f"{fval/1_000_000:.1f}M"
            elif fval >= 1_000:
                formatted = f"{fval/1_000:.1f}k"
            elif fval == int(fval):
                formatted = f"{int(fval):,}"
            else:
                formatted = f"{fval:.2f}"
            
            # Check if it might be a percentage
            field_lower = field.lower()
            if any(word in field_lower for word in ['percent', 'pct', 'rate', 'score']):
                if 0 <= fval <= 1:
                    formatted = f"{fval*100:.0f}%"
                elif 0 <= fval <= 100:
                    formatted = f"{fval:.0f}%"
            
            return f"{display_name}: {formatted}"
            
        except (ValueError, TypeError):
            # Handle non-numeric values
            str_val = str(value)
            if len(str_val) > 20:
                str_val = str_val[:17] + "..."
            return f"{display_name}: {str_val}"

    def _format_filter_line(self, filter_cond, md: bool) -> str:
        """Format a single filter condition for display."""
        field = filter_cond.field.replace('_', ' ').title()
        op = filter_cond.operator
        value = filter_cond.value
        
        # Map operators to readable symbols
        op_map = {
            'gt': '>',
            'gte': '>=',
            'lt': '<',
            'lte': '<=',
            'eq': '=',
            'neq': '≠',
            'contains': 'contains',
            'startswith': 'starts with'
        }
        
        op_symbol = op_map.get(op, op)
        
        # Format the value
        try:
            fval = float(value)
            if fval >= 1000:
                value_str = f"{fval:,.0f}"
            else:
                value_str = str(value)
        except:
            value_str = str(value)
        
        return f"• {field} {op_symbol} {value_str}"

    # Legacy method kept for compatibility
    def _format_row(self, idx: int, row: dict, md: bool) -> str:
        """Original vertical format - kept for reference."""
        b = "**" if md else ""
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
            label = key
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
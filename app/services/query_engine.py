"""
services/query_engine.py  –  100% Deterministic Multi-Tab Query Engine

Receives a StructuredQuery (from the NLP parser) and executes it against
pandas DataFrames loaded from Google Sheets.

Key changes from v1:
  • Works with multiple tabs.  Routes to the correct tab via query.sheet_tab.
  • If sheet_tab is None (ambiguous), searches ALL tabs and merges results.
  • Column names are the REAL sheet column names (no canonical remapping).
  • Numeric-type awareness uses the live schema from SheetsService.
  • Date-aware comparison operators (gt/gte/lt/lte handle date columns).
  • EQ/NEQ coerces value to numeric when column is numeric dtype.
  • Multi-tab aggregation (sum/average/min/max) works correctly.
  • scalar_value is always a raw number — formatting happens at response layer.
  • Validation only runs against the resolved tab, not blindly first tab.
  • Fuzzy value matching on EQ / CONTAINS / IN — handles typos and case.

AI DOES NOT TOUCH THIS FILE.  All logic is explicit, auditable Python.
"""
from __future__ import annotations
import time
import math
from difflib import get_close_matches
import pandas as pd
import numpy as np
from app.models.models import (
    StructuredQuery, QueryResult, FilterOperator,
    AggregationType, OutputFormat
)
from app.services.sheets_service import get_sheets_service
from app.utils.validators import validate_query, ValidationError
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Map operator strings to DataFrame method names for numeric/date comparisons
_OP_METHOD: dict[str, str] = {
    "gt":  "__gt__",
    "gte": "__ge__",
    "lt":  "__lt__",
    "lte": "__le__",
}


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy matching helper  (defined here — no external dependency needed)
# ─────────────────────────────────────────────────────────────────────────────

def _fuzzy_match(
    user_value: str,
    candidates: list[str],
    cutoff: float = 0.75,
) -> str | None:
    """
    Find the closest match for user_value among candidates.

    Strategy (in priority order):
      1. Exact case-insensitive match   → always wins
      2. Prefix match (candidate starts with user_value, unambiguous)
      3. difflib.get_close_matches with given cutoff

    Returns the best matching candidate (original case) or None.
    """
    if not user_value or not candidates:
        return None

    lower_val = user_value.lower().strip()

    # 1. Exact (case-insensitive)
    for c in candidates:
        if c.lower().strip() == lower_val:
            return c

    # 2. Unambiguous prefix
    prefix_hits = [c for c in candidates if c.lower().strip().startswith(lower_val)]
    if len(prefix_hits) == 1:
        return prefix_hits[0]

    # 3. Fuzzy via difflib (stdlib — no extra dependency)
    lower_candidates = [c.lower().strip() for c in candidates]
    close = get_close_matches(lower_val, lower_candidates, n=1, cutoff=cutoff)
    if close:
        idx = lower_candidates.index(close[0])
        return candidates[idx]

    return None


class QueryEngine:
    """
    Executes a StructuredQuery deterministically.

    Pipeline:
      1. Load DataFrames + schema (from cache)
      2. Resolve which tab(s) to query
      3. Validate StructuredQuery against resolved tab schema
      4. Apply filters  →  filtered_df
      5. Apply aggregation  →  QueryResult
    """

    def __init__(self):
        self._sheets = get_sheets_service()

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

    async def execute(self, query: StructuredQuery) -> QueryResult:
        t0 = time.monotonic()

        # 1. Load all data + schema
        all_dfs = await self._sheets.get_all_dataframes()
        schema  = await self._sheets.get_schema()

        if not all_dfs:
            return QueryResult(
                success=False,
                error_message="No data found in the Google Sheet. Check that tabs have data.",
                structured_query=query,
            )

        # 2. Resolve target tab(s)
        if query.sheet_tab:
            if query.sheet_tab not in all_dfs:
                available = ", ".join(f'"{t}"' for t in all_dfs.keys())
                return QueryResult(
                    success=False,
                    error_message=(
                        f"Sheet tab '{query.sheet_tab}' not found.\n"
                        f"Available tabs: {available}"
                    ),
                    structured_query=query,
                )
            tabs_to_query = {query.sheet_tab: all_dfs[query.sheet_tab]}
        else:
            tabs_to_query = all_dfs

        # 3. Validate against schema
        if query.sheet_tab:
            tab_schema     = schema.get(query.sheet_tab, {})
            available_cols = list(tab_schema.keys())
            try:
                validate_query(query, available_cols, tab_schema)
            except ValidationError as exc:
                return QueryResult(
                    success=False,
                    error_message=exc.message,
                    structured_query=query,
                )

        # 4. Apply filters + aggregation
        try:
            if len(tabs_to_query) == 1:
                tab_name, df = next(iter(tabs_to_query.items()))
                filtered     = self._apply_filters(df, query.filters)
                result       = self._aggregate(filtered, query, tab_name)
            else:
                result = self._execute_across_tabs(tabs_to_query, query)
        except Exception as exc:
            logger.error("engine_error", error=str(exc), query=query.intent)
            return QueryResult(
                success=False,
                error_message=f"Error executing query: {exc}",
                structured_query=query,
            )

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        result.execution_time_ms    = elapsed
        result.sheet_last_refreshed = self._sheets.last_refreshed_str()
        result.structured_query     = query

        logger.info(
            "query_executed",
            intent=query.intent,
            tab=query.sheet_tab or "all",
            rows_matched=result.total_rows_matched,
            ms=elapsed,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Multi-tab execution
    # ──────────────────────────────────────────────────────────────────────────

    def _execute_across_tabs(
        self,
        tabs: dict[str, pd.DataFrame],
        query: StructuredQuery,
    ) -> QueryResult:
        combined_frames: list[pd.DataFrame] = []
        total = 0

        for tab_name, df in tabs.items():
            missing = [
                f.field for f in query.filters
                if f.field not in df.columns
            ]
            if missing:
                logger.debug("tab_skipped_missing_cols", tab=tab_name, missing=missing)
                continue

            filtered = self._apply_filters(df, query.filters)
            if filtered.empty:
                continue

            total += len(filtered)
            tagged = filtered.copy()
            tagged.insert(0, "_source_tab", tab_name)
            combined_frames.append(tagged)

        if not combined_frames:
            return QueryResult(success=True, rows=[], total_rows_matched=0)

        combined_df = pd.concat(combined_frames, ignore_index=True)

        if query.aggregation in (AggregationType.COUNT.value, "count"):
            return QueryResult(
                success=True,
                scalar_value=total,
                scalar_label="Total matching records (all tabs)",
                total_rows_matched=total,
            )

        if query.aggregation in (
            AggregationType.SUM.value,        "sum",
            AggregationType.AVERAGE.value,    "average",
            AggregationType.MIN.value,        "min",
            AggregationType.MAX.value,        "max",
            AggregationType.PERCENTAGE.value, "percentage",
        ):
            return self._aggregate(combined_df, query, "all tabs")

        display   = query.display_fields or [c for c in combined_df.columns if c != "_source_tab"]
        show_cols = ["_source_tab"] + [c for c in display if c in combined_df.columns]
        rows      = combined_df[show_cols].to_dict(orient="records")
        return QueryResult(success=True, rows=rows, total_rows_matched=total)

    # ──────────────────────────────────────────────────────────────────────────
    # Filtering
    # ──────────────────────────────────────────────────────────────────────────

    def _apply_filters(self, df: pd.DataFrame, filters: list) -> pd.DataFrame:
        if not filters:
            return df

        mask = pd.Series([True] * len(df), index=df.index)

        for f in filters:
            if f.field not in df.columns:
                logger.warning("filter_field_missing_in_tab", field=f.field)
                continue
            try:
                m = self._apply_single_filter(df[f.field], f.operator, f.value)
            except Exception as exc:
                logger.warning("filter_apply_failed", field=f.field, op=f.operator, error=str(exc))
                continue
            mask = mask & m.fillna(False)

        return df[mask].reset_index(drop=True)

    def _apply_single_filter(
        self,
        col: pd.Series,
        op: str,
        value,
    ) -> pd.Series:
        is_numeric = pd.api.types.is_numeric_dtype(col)

        # ── EQ ───────────────────────────────────────────────────────────────
        if op == FilterOperator.EQ.value:
            if is_numeric:
                return col == pd.to_numeric(value, errors="coerce")

            str_val   = str(value).strip().lower()
            base_mask = col.astype(str).str.strip().str.lower() == str_val
            if base_mask.any():
                return base_mask

            # Fuzzy fallback
            unique_vals = col.dropna().astype(str).unique().tolist()
            best        = _fuzzy_match(str(value), unique_vals, cutoff=0.75)
            if best:
                logger.info("fuzzy_eq_match", user_value=value, matched=best, col=col.name)
                return col.astype(str).str.strip() == best
            return base_mask

        # ── NEQ ──────────────────────────────────────────────────────────────
        elif op == FilterOperator.NEQ.value:
            if is_numeric:
                return col != pd.to_numeric(value, errors="coerce")
            return col.astype(str).str.strip().str.lower() != str(value).strip().lower()

        # ── Range: GT / GTE / LT / LTE ───────────────────────────────────────
        elif op in _OP_METHOD:
            method = _OP_METHOD[op]

            col_dt = pd.to_datetime(col, format="mixed", dayfirst=True, errors="coerce")
            val_dt = pd.to_datetime(str(value), dayfirst=True, errors="coerce")
            if col_dt.notna().mean() > 0.5 and pd.notna(val_dt):
                return getattr(col_dt, method)(val_dt)

            col_num = pd.to_numeric(col, errors="coerce")
            try:
                val_num = float(value)
            except (TypeError, ValueError):
                logger.warning("range_filter_non_numeric_value", value=value, op=op)
                return pd.Series([False] * len(col), index=col.index)
            return getattr(col_num, method)(val_num)

        # ── CONTAINS ─────────────────────────────────────────────────────────
        elif op == FilterOperator.CONTAINS.value:
            str_val = str(value).strip()
            mask    = col.astype(str).str.lower().str.contains(
                str_val.lower(), regex=False, na=False
            )
            if not mask.any() and len(str_val) >= 3:
                unique_vals = col.dropna().astype(str).unique().tolist()
                best        = _fuzzy_match(str_val, unique_vals, cutoff=0.70)
                if best:
                    logger.info("fuzzy_contains_match", user_value=str_val, matched=best, col=col.name)
                    mask = col.astype(str).str.lower().str.contains(
                        best.lower(), regex=False, na=False
                    )
            return mask

        # ── NOT_CONTAINS ─────────────────────────────────────────────────────
        elif op == FilterOperator.NOT_CONTAINS.value:
            return ~col.astype(str).str.lower().str.contains(
                str(value).lower(), regex=False, na=False
            )

        # ── IN ───────────────────────────────────────────────────────────────
        elif op == FilterOperator.IN.value:
            raw_vals        = value if isinstance(value, list) else [value]
            unique_col_vals = col.dropna().astype(str).unique().tolist()

            resolved: list[str] = []
            for v in raw_vals:
                str_v  = str(v).strip()
                direct = [c for c in unique_col_vals if c.lower() == str_v.lower()]
                if direct:
                    resolved.extend(direct)
                else:
                    best = _fuzzy_match(str_v, unique_col_vals, cutoff=0.75)
                    if best:
                        logger.info("fuzzy_in_match", user_value=str_v, matched=best, col=col.name)
                        resolved.append(best)
                    else:
                        resolved.append(str_v)

            resolved_lower = [r.lower() for r in resolved]
            return col.astype(str).str.lower().isin(resolved_lower)

        # ── NOT_IN ───────────────────────────────────────────────────────────
        elif op == FilterOperator.NOT_IN.value:                # ← was IN.value (bug)
            vals = (
                [str(v).lower() for v in value]
                if isinstance(value, list)
                else [str(value).lower()]
            )
            return ~col.astype(str).str.lower().isin(vals)

        else:
            logger.warning("unknown_operator", op=op)
            return pd.Series([True] * len(col), index=col.index)

    # ──────────────────────────────────────────────────────────────────────────
    # Aggregation
    # ──────────────────────────────────────────────────────────────────────────

    def _aggregate(
        self,
        df: pd.DataFrame,
        query: StructuredQuery,
        tab_name: str,
    ) -> QueryResult:
        agg = query.aggregation
        n   = len(df)

        if agg in (AggregationType.COUNT.value, "count"):
            return QueryResult(
                success=True,
                scalar_value=n,
                scalar_label=f"Total matching records in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.SUM.value, "sum"):
            field = query.target_field

            # ── Single-field sum (normal path) ────────────────────────────────
            if field:
                return QueryResult(
                    success=True,
                    scalar_value=self._safe_numeric(df, field, "sum"),
                    scalar_label=f"Sum of '{field}' in '{tab_name}'",
                    total_rows_matched=n,
                )

            # ── Multi-field sum — target_field is None but display_fields has
            #    multiple numeric columns  (e.g. "total cost AND received for X")
            candidate_fields = query.display_fields or list(df.columns)
            sums: dict[str, float] = {}
            for f in candidate_fields:
                if f not in df.columns:
                    continue
                if pd.api.types.is_numeric_dtype(df[f]) or pd.to_numeric(
                    df[f], errors="coerce"
                ).notna().mean() > 0.5:
                    val = self._safe_numeric(df, f, "sum")
                    sums[f] = val

            if not sums:
                return QueryResult(
                    success=False,
                    error_message=(
                        "Aggregation 'sum' requires a numeric target column. "
                        f"None of the display fields {candidate_fields} are numeric."
                    ),
                    structured_query=query,
                    total_rows_matched=n,
                )

            if len(sums) == 1:
                only_field, only_val = next(iter(sums.items()))
                return QueryResult(
                    success=True,
                    scalar_value=only_val,
                    scalar_label=f"Sum of '{only_field}' in '{tab_name}'",
                    total_rows_matched=n,
                )

            # Multiple fields → return as a single summary row so the formatter
            # can display each column with its label and INR formatting intact
            return QueryResult(
                success=True,
                rows=[sums],
                scalar_label=f"Totals in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.AVERAGE.value, "average"):
            field = query.target_field
            return QueryResult(
                success=True,
                scalar_value=self._safe_numeric(df, field, "mean"),
                scalar_label=f"Average of '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.MIN.value, "min"):
            field = query.target_field
            return QueryResult(
                success=True,
                scalar_value=self._safe_numeric(df, field, "min"),
                scalar_label=f"Minimum '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.MAX.value, "max"):
            field = query.target_field
            return QueryResult(
                success=True,
                scalar_value=self._safe_numeric(df, field, "max"),
                scalar_label=f"Maximum '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.PERCENTAGE.value, "percentage"):
            num_total = self._safe_numeric(df, query.numerator_field,   "sum")
            den_total = self._safe_numeric(df, query.denominator_field, "sum")
            pct = round((num_total / den_total * 100) if den_total else 0.0, 2)
            return QueryResult(
                success=True,
                scalar_value=pct,
                scalar_label=(
                    f"% of '{query.numerator_field}' vs '{query.denominator_field}'"
                    f" in '{tab_name}'"
                ),
                total_rows_matched=n,
            )

        else:
            # LIST / TABLE
            display = query.display_fields or list(df.columns)
            display = [c for c in display if c in df.columns]
            rows    = (
                df[display].to_dict(orient="records")
                if display
                else df.to_dict(orient="records")
            )
            return QueryResult(success=True, rows=rows, total_rows_matched=n)

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_numeric(
        df: pd.DataFrame,
        field: str | None,
        operation: str,
    ) -> float:
        if not field or field not in df.columns:
            return 0.0
        series = pd.to_numeric(df[field], errors="coerce")
        if series.isna().all():
            return 0.0
        result = getattr(series, operation)()
        return float(result) if not math.isnan(result) else 0.0

    @staticmethod
    def format_scalar(value: float | int | None) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return "N/A"
        if isinstance(value, float) and value == int(value):
            return f"{int(value):,}"
        return f"{value:,.2f}"


# ─────────────────────────────────────────────────────────────────────────────
_engine: QueryEngine | None = None


def get_query_engine() -> QueryEngine:
    global _engine
    if _engine is None:
        _engine = QueryEngine()
    return _engine
"""
services/query_engine.py  –  100% Deterministic Multi-Tab Query Engine

Receives a StructuredQuery (from the NLP parser) and executes it against
pandas DataFrames loaded from Google Sheets.

Key changes from v1:
  • Works with multiple tabs.  Routes to the correct tab via query.sheet_tab.
  • If sheet_tab is None (ambiguous), searches ALL tabs and merges results.
  • Column names are the REAL sheet column names (no canonical remapping).
  • Numeric-type awareness uses the live schema from SheetsService.

AI DOES NOT TOUCH THIS FILE.  All logic is explicit, auditable Python.
"""
from __future__ import annotations
import time
import math
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


class QueryEngine:
    """
    Executes a StructuredQuery deterministically.

    Pipeline:
      1. Load DataFrames + schema (from cache)
      2. Resolve which tab(s) to query
      3. Validate StructuredQuery against schema
      4. Apply filters  →  filtered_df
      5. Apply aggregation  →  QueryResult
    """

    def __init__(self):
        self._sheets = get_sheets_service()

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

    def execute(self, query: StructuredQuery) -> QueryResult:
        t0 = time.monotonic()

        # 1. Load all data + schema
        all_dfs = self._sheets.get_all_dataframes()
        schema  = self._sheets.get_schema()

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
            # Tab not specified — search all tabs
            tabs_to_query = all_dfs

        # 3. Validate against schema of target tab(s)
        # Use the first (or only) tab's columns for validation
        first_tab  = next(iter(tabs_to_query))
        tab_schema = schema.get(first_tab, {})
        available_cols = list(tab_schema.keys())

        try:
            validate_query(query, available_cols, tab_schema)
        except ValidationError as exc:
            return QueryResult(
                success=False,
                error_message=exc.message,
                structured_query=query,
            )

        # 4. Apply filters + aggregation across target tabs
        try:
            if len(tabs_to_query) == 1:
                tab_name, df = next(iter(tabs_to_query.items()))
                filtered = self._apply_filters(df, query.filters)
                result   = self._aggregate(filtered, query, tab_name)
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
    # Multi-tab execution  –  union results across all tabs
    # ──────────────────────────────────────────────────────────────────────────

    def _execute_across_tabs(
        self,
        tabs: dict[str, pd.DataFrame],
        query: StructuredQuery,
    ) -> QueryResult:
        """
        Run the same query against every tab and union the results.
        A tab is skipped silently if it doesn't contain the filtered columns.
        Adds a "_source_tab" column to each result row so the user knows
        which sheet each record came from.
        """
        combined_rows: list[dict] = []
        total = 0

        for tab_name, df in tabs.items():
            # Skip tabs that don't have all the filtered fields
            missing = [
                f.field for f in query.filters
                if f.field not in df.columns
            ]
            if missing:
                logger.debug("tab_skipped_missing_cols", tab=tab_name, missing=missing)
                continue

            filtered = self._apply_filters(df, query.filters)
            total   += len(filtered)

            if len(filtered) == 0:
                continue

            # Add provenance column
            filtered = filtered.copy()
            filtered.insert(0, "_source_tab", tab_name)

            display = query.display_fields or list(df.columns)
            # Always include _source_tab
            show_cols = ["_source_tab"] + [c for c in display if c in filtered.columns]
            combined_rows.extend(filtered[show_cols].to_dict(orient="records"))

        # For count queries: return total across all tabs
        if query.aggregation in (AggregationType.COUNT.value, "count"):
            return QueryResult(
                success=True,
                scalar_value=total,
                scalar_label="Total matching records (all tabs)",
                total_rows_matched=total,
            )

        return QueryResult(
            success=True,
            rows=combined_rows,
            total_rows_matched=total,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Filtering
    # ──────────────────────────────────────────────────────────────────────────

    def _apply_filters(self, df: pd.DataFrame, filters: list) -> pd.DataFrame:
        if not filters:
            return df

        mask = pd.Series([True] * len(df), index=df.index)

        for f in filters:
            field = f.field
            op    = f.operator
            value = f.value

            if field not in df.columns:
                logger.warning("filter_field_missing_in_tab", field=field)
                continue

            col = df[field]

            if op == FilterOperator.EQ.value:
                if pd.api.types.is_numeric_dtype(col):
                    m = col == value
                else:
                    m = col.astype(str).str.strip().str.lower() == str(value).lower()

            elif op == FilterOperator.NEQ.value:
                if pd.api.types.is_numeric_dtype(col):
                    m = col != value
                else:
                    m = col.astype(str).str.strip().str.lower() != str(value).lower()

            elif op == FilterOperator.GT.value:
                m = pd.to_numeric(col, errors="coerce") > float(value)

            elif op == FilterOperator.GTE.value:
                m = pd.to_numeric(col, errors="coerce") >= float(value)

            elif op == FilterOperator.LT.value:
                m = pd.to_numeric(col, errors="coerce") < float(value)

            elif op == FilterOperator.LTE.value:
                m = pd.to_numeric(col, errors="coerce") <= float(value)

            elif op == FilterOperator.CONTAINS.value:
                m = col.astype(str).str.lower().str.contains(
                    str(value).lower(), regex=False, na=False
                )

            elif op == FilterOperator.NOT_CONTAINS.value:
                m = ~col.astype(str).str.lower().str.contains(
                    str(value).lower(), regex=False, na=False
                )

            elif op == FilterOperator.IN.value:
                vals = [str(v).lower() for v in value] if isinstance(value, list) else [str(value).lower()]
                m = col.astype(str).str.lower().isin(vals)

            elif op == FilterOperator.NOT_IN.value:
                vals = [str(v).lower() for v in value] if isinstance(value, list) else [str(value).lower()]
                m = ~col.astype(str).str.lower().isin(vals)

            else:
                logger.warning("unknown_operator", op=op)
                continue

            mask = mask & m.fillna(False)

        return df[mask].reset_index(drop=True)

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
            total = self._safe_sum(df, field)
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(total),
                scalar_label=f"Sum of '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.AVERAGE.value, "average"):
            field = query.target_field
            avg   = pd.to_numeric(df[field], errors="coerce").mean() if field and field in df.columns else 0
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(avg),
                scalar_label=f"Average of '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.MIN.value, "min"):
            field = query.target_field
            val   = pd.to_numeric(df[field], errors="coerce").min() if field and field in df.columns else None
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(val),
                scalar_label=f"Minimum '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.MAX.value, "max"):
            field = query.target_field
            val   = pd.to_numeric(df[field], errors="coerce").max() if field and field in df.columns else None
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(val),
                scalar_label=f"Maximum '{field}' in '{tab_name}'",
                total_rows_matched=n,
            )

        elif agg in (AggregationType.PERCENTAGE.value, "percentage"):
            num_total = self._safe_sum(df, query.numerator_field)
            den_total = self._safe_sum(df, query.denominator_field)
            pct = round((num_total / den_total * 100) if den_total else 0, 2)
            return QueryResult(
                success=True,
                scalar_value=pct,
                scalar_label=(
                    f"% of '{query.numerator_field}' vs '{query.denominator_field}'"
                    f" in '{tab_name}'"
                ),
                total_rows_matched=n,
            )

        else:  # LIST / TABLE (default)
            display = query.display_fields or list(df.columns)
            display = [c for c in display if c in df.columns]
            rows    = df[display].to_dict(orient="records") if display else df.to_dict(orient="records")
            return QueryResult(
                success=True,
                rows=rows,
                total_rows_matched=n,
            )

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_sum(df: pd.DataFrame, field: str | None) -> float:
        if not field or field not in df.columns:
            return 0.0
        return float(pd.to_numeric(df[field], errors="coerce").sum())

    @staticmethod
    def _fmt_number(value) -> str:
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
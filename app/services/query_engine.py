"""
services/query_engine.py  –  100% Deterministic Query Engine

This module is the heart of the accuracy guarantee.
It receives a StructuredQuery (produced by the NLP parser) and executes
it against a pandas DataFrame using pure Python logic.

AI DOES NOT TOUCH THIS FILE.  Every filter, aggregation, and calculation
is explicit, auditable code.  The output is always derived 1:1 from the
sheet data.
"""
from __future__ import annotations
import time
import math
import pandas as pd
import numpy as np
from models import (
    StructuredQuery, QueryResult, FilterOperator,
    AggregationType, OutputFormat
)
from services.sheets_service import get_sheets_service
from utils.validators import validate_query, ValidationError
from utils.logger import get_logger

logger = get_logger(__name__)


class QueryEngine:
    """
    Executes a StructuredQuery deterministically.

    Pipeline:
      1. Load DataFrame (from cache or API)
      2. Validate query against available columns
      3. Apply filters  →  filtered_df
      4. Apply aggregation  →  result
      5. Build QueryResult
    """

    def __init__(self):
        self._sheets = get_sheets_service()

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

    def execute(self, query: StructuredQuery) -> QueryResult:
        t0 = time.monotonic()

        # 1. Load data
        df = self._sheets.get_dataframe()
        if df.empty:
            return QueryResult(
                success=False,
                error_message="The Google Sheet appears to be empty.",
                structured_query=query,
            )

        # 2. Remap canonical columns → actual sheet columns
        col_map = self._sheets.get_canonical_columns()
        df_mapped = self._remap_columns(df, col_map)
        available = list(df_mapped.columns)

        # 3. Validate query
        try:
            validate_query(query, available)
        except ValidationError as exc:
            return QueryResult(
                success=False,
                error_message=exc.message,
                structured_query=query,
            )

        # 4. Apply filters
        try:
            filtered = self._apply_filters(df_mapped, query.filters)
        except Exception as exc:
            logger.error("filter_error", error=str(exc), query=query.intent)
            return QueryResult(
                success=False,
                error_message=f"Error applying filters: {exc}",
                structured_query=query,
            )

        # 5. Apply aggregation
        try:
            result = self._aggregate(filtered, query, col_map)
        except Exception as exc:
            logger.error("aggregation_error", error=str(exc), query=query.intent)
            return QueryResult(
                success=False,
                error_message=f"Error computing result: {exc}",
                structured_query=query,
            )

        elapsed = (time.monotonic() - t0) * 1000
        result.execution_time_ms = round(elapsed, 1)
        result.sheet_last_refreshed = self._sheets.last_refreshed_str()
        result.structured_query = query
        logger.info(
            "query_executed",
            intent=query.intent,
            rows_matched=result.total_rows_matched,
            ms=result.execution_time_ms,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Column remapping
    # ──────────────────────────────────────────────────────────────────────────

    def _remap_columns(self, df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
        """
        Create a working copy of the DataFrame with canonical column names.
        Only columns present in the sheet are included.
        Original column names are preserved as fallback.
        """
        rename = {actual: canon for canon, actual in col_map.items() if actual in df.columns}
        df2 = df.rename(columns=rename)
        return df2

    # ──────────────────────────────────────────────────────────────────────────
    # Filtering  –  pure Pandas, no AI
    # ──────────────────────────────────────────────────────────────────────────

    def _apply_filters(
        self,
        df: pd.DataFrame,
        filters: list,
    ) -> pd.DataFrame:
        if not filters:
            return df

        mask = pd.Series([True] * len(df), index=df.index)

        for f in filters:
            field = f.field
            op    = f.operator
            value = f.value

            if field not in df.columns:
                logger.warning("filter_field_missing", field=field)
                continue

            col = df[field]

            if op == FilterOperator.EQ.value:
                # Case-insensitive string match OR numeric equality
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
    # Aggregation  –  pure Python/NumPy, no AI
    # ──────────────────────────────────────────────────────────────────────────

    def _aggregate(
        self,
        df: pd.DataFrame,
        query: StructuredQuery,
        col_map: dict[str, str],
    ) -> QueryResult:
        agg = query.aggregation
        n   = len(df)

        if agg == AggregationType.COUNT.value:
            return QueryResult(
                success=True,
                scalar_value=n,
                scalar_label="Total matching records",
                total_rows_matched=n,
            )

        elif agg == AggregationType.SUM.value:
            field = query.target_field
            total = self._safe_sum(df, field)
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(total),
                scalar_label=f"Sum of {field}",
                total_rows_matched=n,
            )

        elif agg == AggregationType.AVERAGE.value:
            field = query.target_field
            col = pd.to_numeric(df[field], errors="coerce")
            avg = col.mean()
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(avg),
                scalar_label=f"Average {field}",
                total_rows_matched=n,
            )

        elif agg == AggregationType.MIN.value:
            field = query.target_field
            col = pd.to_numeric(df[field], errors="coerce")
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(col.min()),
                scalar_label=f"Min {field}",
                total_rows_matched=n,
            )

        elif agg == AggregationType.MAX.value:
            field = query.target_field
            col = pd.to_numeric(df[field], errors="coerce")
            return QueryResult(
                success=True,
                scalar_value=self._fmt_number(col.max()),
                scalar_label=f"Max {field}",
                total_rows_matched=n,
            )

        elif agg == AggregationType.PERCENTAGE.value:
            num_field = query.numerator_field
            den_field = query.denominator_field
            num_total = self._safe_sum(df, num_field)
            den_total = self._safe_sum(df, den_field)
            pct = (num_total / den_total * 100) if den_total else 0
            return QueryResult(
                success=True,
                scalar_value=round(pct, 2),
                scalar_label=f"% {num_field} / {den_field}",
                total_rows_matched=n,
            )

        else:  # LIST / TABLE (default)
            display = query.display_fields or list(df.columns)
            # Keep only columns that actually exist
            display = [c for c in display if c in df.columns]
            rows = df[display].to_dict(orient="records")
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
    def _fmt_number(value: float) -> str:
        """Format large numbers with commas for readability."""
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return "N/A"
        if isinstance(value, float) and value == int(value):
            return f"{int(value):,}"
        return f"{value:,.2f}"


# Module-level singleton
_engine: QueryEngine | None = None


def get_query_engine() -> QueryEngine:
    global _engine
    if _engine is None:
        _engine = QueryEngine()
    return _engine
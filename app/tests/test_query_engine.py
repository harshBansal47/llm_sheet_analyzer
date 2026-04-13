"""
tests/test_query_engine.py  –  Unit tests for the deterministic query engine.

v2: Tests updated for multi-tab architecture.
  • Mock SheetsService returns dict[tab_name → DataFrame]
  • Tests cover single-tab queries, multi-tab search, and tab routing
  • No Google Sheets or OpenAI access required
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock

from app.models.models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType,
)
from app.services.query_engine import QueryEngine

# ─── Mock data ───────────────────────────────────────────────────────────────

PAYMENTS_DATA = {
    "Customer Name": ["Ahmed Ali", "Sara Khan", "Ravi Patel", "Noor Fatima", "James Bond"],
    "Unit":          ["A-101",    "B-202",     "A-302",      "C-101",       "B-305"],
    "Phase":         ["Phase 1",  "Phase 2",   "Phase 1",    "Phase 2",     "Phase 2"],
    "Total Cost":    [1_000_000,  1_500_000,   900_000,      1_200_000,     800_000],
    "Amount Received":[700_000,   400_000,     810_000,      240_000,       200_000],
    "Payment %":     [70.0,       26.67,       90.0,         20.0,          25.0],
    "Status":        ["Active",   "Active",    "Active",     "Court Case",  "Court Case"],
}

COURT_CASES_DATA = {
    "Customer Name": ["Noor Fatima", "James Bond", "Zara Shah"],
    "Unit":          ["C-101",       "B-305",      "D-201"],
    "Case Number":   ["CC-001",      "CC-002",     "CC-003"],
    "Filed Date":    ["2024-01-15",  "2024-03-10", "2024-06-01"],
    "Amount Disputed":[240_000,      200_000,      500_000],
    "Case Status":   ["Active",      "Active",     "Settled"],
}

SCHEMA = {
    "Payments": {
        "Customer Name": "text",
        "Unit": "text",
        "Phase": "text",
        "Total Cost": "numeric",
        "Amount Received": "numeric",
        "Payment %": "numeric",
        "Status": "text",
    },
    "Court Cases": {
        "Customer Name": "text",
        "Unit": "text",
        "Case Number": "text",
        "Filed Date": "text",
        "Amount Disputed": "numeric",
        "Case Status": "text",
    },
}


def make_engine() -> QueryEngine:
    """Return a QueryEngine backed by mock multi-tab data."""
    engine = QueryEngine.__new__(QueryEngine)
    mock   = MagicMock()

    all_dfs = {
        "Payments":    pd.DataFrame(PAYMENTS_DATA),
        "Court Cases": pd.DataFrame(COURT_CASES_DATA),
    }
    mock.get_all_dataframes.return_value = {k: v.copy() for k, v in all_dfs.items()}
    mock.get_schema.return_value         = SCHEMA
    mock.last_refreshed_str.return_value = "12:00:00"
    engine._sheets = mock
    return engine


# ─── Single-tab filter tests ─────────────────────────────────────────────────

class TestSingleTabFiltering:

    def test_eq_exact_match(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Customer Name", operator="eq", value="Ahmed Ali")],
            aggregation="list", display_fields=["Customer Name", "Payment %"],
        )
        r = engine.execute(q)
        assert r.success
        assert r.total_rows_matched == 1
        assert r.rows[0]["Customer Name"] == "Ahmed Ali"

    def test_eq_case_insensitive(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Customer Name", operator="eq", value="ahmed ali")],
            aggregation="list", display_fields=["Customer Name"],
        )
        r = engine.execute(q)
        assert r.total_rows_matched == 1

    def test_gt_numeric(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Payment %", operator="gt", value=60)],
            aggregation="list", display_fields=["Customer Name", "Payment %"],
        )
        r = engine.execute(q)
        # Ahmed (70%) and Ravi (90%)
        assert r.total_rows_matched == 2
        names = {row["Customer Name"] for row in r.rows}
        assert names == {"Ahmed Ali", "Ravi Patel"}

    def test_lt_numeric(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Payment %", operator="lt", value=30)],
            aggregation="list", display_fields=["Customer Name", "Payment %"],
        )
        r = engine.execute(q)
        # Sara (26.67), Noor (20), James (25)
        assert r.total_rows_matched == 3

    def test_contains_filter(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Status", operator="contains", value="court")],
            aggregation="list", display_fields=["Customer Name", "Status"],
        )
        r = engine.execute(q)
        assert r.total_rows_matched == 2

    def test_multi_filter(self):
        """Phase 2 AND payment < 30% AND court case"""
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[
                FilterCondition(field="Phase",     operator="eq",       value="Phase 2"),
                FilterCondition(field="Payment %", operator="lt",       value=30),
                FilterCondition(field="Status",    operator="contains", value="Court"),
            ],
            aggregation="list",
            display_fields=["Customer Name", "Phase", "Payment %", "Status"],
        )
        r = engine.execute(q)
        # Noor (Phase2, 20%, Court) and James (Phase2, 25%, Court)
        assert r.total_rows_matched == 2

    def test_no_filters_returns_all(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[], aggregation="list",
        )
        r = engine.execute(q)
        assert r.total_rows_matched == 5


# ─── Aggregation tests ───────────────────────────────────────────────────────

class TestAggregation:

    def test_count(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Phase", operator="eq", value="Phase 2")],
            aggregation="count",
        )
        r = engine.execute(q)
        assert r.success
        assert r.scalar_value == 3   # Sara, Noor, James

    def test_sum(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[], aggregation="sum", target_field="Total Cost",
        )
        r = engine.execute(q)
        # 1M + 1.5M + 0.9M + 1.2M + 0.8M = 5,400,000
        assert r.scalar_value == "5,400,000"

    def test_count_zero_results(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Payments",
            filters=[FilterCondition(field="Phase", operator="eq", value="Phase 99")],
            aggregation="count",
        )
        r = engine.execute(q)
        assert r.scalar_value == 0


# ─── Multi-tab tests ─────────────────────────────────────────────────────────

class TestMultiTab:

    def test_second_tab_query(self):
        """Query the Court Cases tab directly."""
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="Court Cases",
            filters=[FilterCondition(field="Case Status", operator="eq", value="Active")],
            aggregation="list",
            display_fields=["Customer Name", "Case Number", "Case Status"],
        )
        r = engine.execute(q)
        assert r.success
        assert r.total_rows_matched == 2
        names = {row["Customer Name"] for row in r.rows}
        assert names == {"Noor Fatima", "James Bond"}

    def test_cross_tab_search(self):
        """sheet_tab=None → search all tabs, results tagged with _source_tab."""
        engine = make_engine()
        # Search "Noor Fatima" across all tabs — she appears in both
        q = StructuredQuery(
            intent="test", sheet_tab=None,
            filters=[FilterCondition(field="Customer Name", operator="eq", value="Noor Fatima")],
            aggregation="list",
            display_fields=["Customer Name"],
        )
        r = engine.execute(q)
        assert r.success
        assert r.total_rows_matched == 2   # one row in each tab
        tabs_found = {row["_source_tab"] for row in r.rows}
        assert tabs_found == {"Payments", "Court Cases"}

    def test_invalid_tab_returns_error(self):
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab="NonExistent",
            filters=[], aggregation="list",
        )
        r = engine.execute(q)
        assert not r.success
        assert "not found" in r.error_message.lower()

    def test_cross_tab_count(self):
        """Count across all tabs for a field present in both."""
        engine = make_engine()
        q = StructuredQuery(
            intent="test", sheet_tab=None,
            filters=[FilterCondition(field="Customer Name", operator="contains", value="James")],
            aggregation="count",
        )
        r = engine.execute(q)
        assert r.success
        assert r.scalar_value == 2   # James Bond in Payments + Court Cases
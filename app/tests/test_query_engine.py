"""
tests/test_query_engine.py  –  Unit tests for the deterministic query engine.

These tests do NOT require Google Sheets or OpenAI access.
They inject a mock DataFrame directly into the engine to verify
that every filter operator, aggregation, and edge case works correctly.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from app.models.models import (
    StructuredQuery, FilterCondition, FilterOperator,
    AggregationType, OutputFormat
)
from app.services.query_engine import QueryEngine


# ─── Fixtures ────────────────────────────────────────────────────────────────

SAMPLE_DATA = {
    "customer_name": ["Ahmed Ali", "Sara Khan", "Ravi Patel", "Noor Fatima", "James Bond"],
    "unit":          ["A-101", "B-202", "A-302", "C-101", "B-305"],
    "phase":         ["Phase 1", "Phase 2", "Phase 1", "Phase 2", "Phase 2"],
    "total_cost":    [1_000_000, 1_500_000, 900_000, 1_200_000, 800_000],
    "amount_received":[700_000,   400_000,  810_000,  240_000,  200_000],
    "payment_percent":[70.0,      26.67,    90.0,     20.0,     25.0],
    "status":        ["Active", "Active", "Active", "Court Case", "Court Case"],
    "remarks":       ["", "", "Paid on time", "Dispute pending", ""],
}


def make_engine_with_mock_data() -> QueryEngine:
    """Return a QueryEngine that serves SAMPLE_DATA instead of real Sheets."""
    engine = QueryEngine.__new__(QueryEngine)
    mock_sheets = MagicMock()
    mock_sheets.get_dataframe.return_value = pd.DataFrame(SAMPLE_DATA)
    mock_sheets.get_canonical_columns.return_value = {
        k: k for k in SAMPLE_DATA.keys()   # identity mapping for tests
    }
    mock_sheets.last_refreshed_str.return_value = "12:00:00"
    engine._sheets = mock_sheets
    return engine


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestFiltering:

    def test_filter_eq_string(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="customer_name", operator="eq", value="Ahmed Ali")],
            aggregation="list",
            display_fields=["customer_name", "payment_percent"],
        )
        result = engine.execute(query)
        assert result.success
        assert result.total_rows_matched == 1
        assert result.rows[0]["customer_name"] == "Ahmed Ali"

    def test_filter_eq_case_insensitive(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="customer_name", operator="eq", value="ahmed ali")],
            aggregation="list",
            display_fields=["customer_name"],
        )
        result = engine.execute(query)
        assert result.total_rows_matched == 1

    def test_filter_gt_numeric(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="payment_percent", operator="gt", value=60)],
            aggregation="list",
            display_fields=["customer_name", "payment_percent"],
        )
        result = engine.execute(query)
        # Ahmed (70%) and Ravi (90%)
        assert result.total_rows_matched == 2
        names = {r["customer_name"] for r in result.rows}
        assert names == {"Ahmed Ali", "Ravi Patel"}

    def test_filter_lt_numeric(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="payment_percent", operator="lt", value=30)],
            aggregation="list",
            display_fields=["customer_name", "payment_percent"],
        )
        result = engine.execute(query)
        # Sara(26.67), Noor(20), James(25)
        assert result.total_rows_matched == 3

    def test_filter_contains(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="status", operator="contains", value="court")],
            aggregation="list",
            display_fields=["customer_name", "status"],
        )
        result = engine.execute(query)
        assert result.total_rows_matched == 2

    def test_multi_filter(self):
        """Phase 2 AND payment_percent > 20 AND status contains court"""
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[
                FilterCondition(field="phase",           operator="eq",       value="Phase 2"),
                FilterCondition(field="payment_percent", operator="lt",       value=30),
                FilterCondition(field="status",          operator="contains", value="Court"),
            ],
            aggregation="list",
            display_fields=["customer_name", "phase", "payment_percent", "status"],
        )
        result = engine.execute(query)
        # Noor(Phase2, 20%, Court) and James(Phase2, 25%, Court)
        assert result.total_rows_matched == 2


class TestAggregation:

    def test_count(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="phase", operator="eq", value="Phase 2")],
            aggregation="count",
        )
        result = engine.execute(query)
        assert result.success
        assert result.scalar_value == 3   # Sara, Noor, James

    def test_sum(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[],
            aggregation="sum",
            target_field="total_cost",
        )
        result = engine.execute(query)
        assert result.success
        # 1M + 1.5M + 0.9M + 1.2M + 0.8M = 5,400,000
        assert result.scalar_value == "5,400,000"

    def test_count_zero(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[FilterCondition(field="phase", operator="eq", value="Phase 99")],
            aggregation="count",
        )
        result = engine.execute(query)
        assert result.scalar_value == 0

    def test_list_all(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test",
            filters=[],
            aggregation="list",
            display_fields=["customer_name"],
        )
        result = engine.execute(query)
        assert result.total_rows_matched == 5


class TestEdgeCases:

    def test_no_filters_returns_all(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="test", filters=[], aggregation="list",
            display_fields=["customer_name"],
        )
        result = engine.execute(query)
        assert result.total_rows_matched == 5

    def test_specific_unit_lookup(self):
        engine = make_engine_with_mock_data()
        query = StructuredQuery(
            intent="unit_lookup",
            filters=[FilterCondition(field="unit", operator="eq", value="A-302")],
            aggregation="list",
            display_fields=["customer_name", "unit", "total_cost", "amount_received"],
        )
        result = engine.execute(query)
        assert result.total_rows_matched == 1
        assert result.rows[0]["customer_name"] == "Ravi Patel"
        assert result.rows[0]["total_cost"] == 900_000
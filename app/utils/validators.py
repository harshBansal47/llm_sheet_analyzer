"""
utils/validators.py  –  Pre-execution validation of StructuredQuery.

Runs BEFORE the query engine so we catch bad parses early and return
a helpful clarification instead of a wrong answer.
"""
from models import StructuredQuery, FilterCondition, AggregationType


NUMERIC_FIELDS = {"payment_percent", "total_cost", "amount_received"}
NUMERIC_OPERATORS = {"gt", "gte", "lt", "lte"}


class ValidationError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def validate_query(query: StructuredQuery, available_columns: list[str]) -> None:
    """
    Raises ValidationError with a user-friendly message if the query is
    structurally invalid or references non-existent fields.
    """
    # 1. Confidence gate
    if query.confidence < 0.60:
        raise ValidationError(
            "I'm not confident I understood your question correctly. "
            "Could you rephrase it? "
            f"(Hint: {query.clarification_message})"
        )

    # 2. Clarification required by parser
    if query.clarification_needed:
        raise ValidationError(query.clarification_message)

    # 3. Validate filter fields exist
    for f in query.filters:
        if f.field not in available_columns:
            close = _suggest_close(f.field, available_columns)
            raise ValidationError(
                f"Column '{f.field}' not found in the sheet. "
                + (f"Did you mean '{close}'?" if close else "")
            )

    # 4. Validate numeric operators are on numeric fields
    for f in query.filters:
        if f.operator in NUMERIC_OPERATORS and f.field not in NUMERIC_FIELDS:
            raise ValidationError(
                f"Cannot apply numeric comparison on non-numeric field '{f.field}'."
            )

    # 5. Aggregation-specific checks
    agg = query.aggregation
    if agg in ("sum", "average", "min", "max") and not query.target_field:
        raise ValidationError(
            f"Aggregation '{agg}' requires specifying which field to compute."
        )
    if agg == "percentage":
        if not query.numerator_field or not query.denominator_field:
            raise ValidationError(
                "Percentage calculation needs both a numerator and denominator field."
            )

    # 6. Empty filter warning (returns all rows – warn but allow)
    # This is intentional for queries like "list all customers"


def _suggest_close(field: str, available: list[str]) -> str | None:
    """Very simple Levenshtein-like suggestion."""
    field_lower = field.lower()
    for col in available:
        if field_lower in col.lower() or col.lower() in field_lower:
            return col
    return None
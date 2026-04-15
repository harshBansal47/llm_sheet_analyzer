"""
utils/validators.py  –  Pre-execution validation of StructuredQuery.

Runs BEFORE the query engine so bad parses are caught early and return a
helpful clarification message instead of a wrong or confusing answer.

v2 changes:
  • Accepts live tab_schema dict { col_name: "numeric"|"text" } instead of
    a hardcoded list of canonical field names.
  • Numeric-operator check uses the inferred type from the schema.
"""
from app.models.models import StructuredQuery, FilterCondition

NUMERIC_OPERATORS = {"gt", "gte", "lt", "lte"}


class ValidationError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

def validate_query(
    query: StructuredQuery,
    available_columns: list[str],
    tab_schema: dict[str, dict] | None = None,
) -> None:
    """
    Validate StructuredQuery against available columns and schema.

    Supports both:
    - old schema: { col: "numeric" }
    - new schema: { col: { "type": "numeric", ... } }
    """

    tab_schema = tab_schema or {}

    # ─────────────────────────────────────────────
    # Helper: extract column type safely
    # ─────────────────────────────────────────────
    def _get_col_type(field: str) -> str | None:
        meta = tab_schema.get(field)

        if isinstance(meta, str):      # backward compatibility
            return meta

        if isinstance(meta, dict):     # new schema
            return meta.get("type")

        return None

    # ─────────────────────────────────────────────
    # 1. Confidence gate
    # ─────────────────────────────────────────────
    if query.confidence < 0.60:
        raise ValidationError(
            "I'm not confident I understood your question correctly. "
            "Could you rephrase it?"
            + (f"\n(Hint: {query.clarification_message})" if query.clarification_message else "")
        )

    # ─────────────────────────────────────────────
    # 2. Parser clarification
    # ─────────────────────────────────────────────
    if query.clarification_needed:
        raise ValidationError(query.clarification_message or "Please clarify your question.")

    # ─────────────────────────────────────────────
    # 3. Column existence check
    # ─────────────────────────────────────────────
    for f in query.filters:
        if f.field not in available_columns:
            close = _suggest_close(f.field, available_columns)
            raise ValidationError(
                f"Column \"{f.field}\" not found in this sheet."
                + (f" Did you mean \"{close}\"?" if close else "")
            )

    # ─────────────────────────────────────────────
    # 4. Numeric operator validation
    # ─────────────────────────────────────────────
    if tab_schema:
        for f in query.filters:
            op = f.operator if isinstance(f.operator, str) else f.operator.value

            if op in NUMERIC_OPERATORS:
                col_type = _get_col_type(f.field)

                if col_type == "empty":
                    raise ValidationError(
                        f"Column \"{f.field}\" has no data yet, cannot apply numeric filter."
                    )

                if col_type != "numeric":
                    raise ValidationError(
                        f"Cannot apply numeric comparison ('{op}') on "
                        f"non-numeric column \"{f.field}\"."
                    )

    # ─────────────────────────────────────────────
    # 5. Aggregation validation
    # ─────────────────────────────────────────────
    agg = query.aggregation if isinstance(query.aggregation, str) else query.aggregation.value

    if agg in ("sum", "average", "min", "max") and not query.target_field:
        raise ValidationError(
            f"Aggregation '{agg}' requires a target column. "
            "Please specify which column to compute."
        )

    if agg == "percentage":
        if not query.numerator_field or not query.denominator_field:
            raise ValidationError(
                "Percentage calculation needs both a numerator column and a denominator column."
            )

        if tab_schema:
            for field_name in (query.numerator_field, query.denominator_field):
                if field_name:
                    col_type = _get_col_type(field_name)

                    if col_type != "numeric":
                        raise ValidationError(
                            f"Percentage calculation: column \"{field_name}\" must be numeric."
                        )


def _suggest_close(field: str, available: list[str]) -> str | None:
    """Simple substring-based suggestion."""
    fl = field.lower()
    for col in available:
        if fl in col.lower() or col.lower() in fl:
            return col
    return None
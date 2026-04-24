"""
utils/validators.py  –  Pre-execution validation of StructuredQuery.
"""
from difflib import get_close_matches
from app.models.models import StructuredQuery, FilterCondition

# Operators that require the column to hold comparable (ordered) values
RANGE_OPERATORS = {"gt", "gte", "lt", "lte"}

# Column types that support range / numeric operators
RANGE_OPERATOR_ALLOWED_TYPES = {
    "numeric",
    "percentage",
    "currency",
    "identifier",
    "date",
}

# Column types that can be used in sum/average/percentage aggregations
AGGREGATABLE_TYPES = {
    "numeric",
    "currency",
    "percentage",
}


def _to_number(val: float) -> int | float:
    return int(val) if val == int(val) else round(val, 4)


class ValidationError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def validate_query(
    query: StructuredQuery,
    available_columns: list[str],
    tab_schema: dict[str, dict] | None = None,
) -> None:
    tab_schema = tab_schema or {}

    def _get_col_type(field: str) -> str | None:
        meta = tab_schema.get(field)
        if isinstance(meta, str):
            return meta
        if isinstance(meta, dict):
            return meta.get("type")
        return None

    # ── 1. Confidence gate ────────────────────────────────────────────────────
    if query.confidence < 0.60:
        raise ValidationError(
            "I'm not confident I understood your question correctly. "
            "Could you rephrase it?"
            + (f"\n(Hint: {query.clarification_message})" if query.clarification_message else "")
        )

    # ── 2. Parser clarification ───────────────────────────────────────────────
    if query.clarification_needed:
        raise ValidationError(query.clarification_message or "Please clarify your question.")

    # ── 3. Column existence ───────────────────────────────────────────────────
    for f in query.filters:
        if f.field not in available_columns:
            close = _suggest_close(f.field, available_columns)
            raise ValidationError(
                f"Column \"{f.field}\" not found in this sheet."
                + (f" Did you mean \"{close}\"?" if close else "")
            )

    # ── 4. Range operator type check ──────────────────────────────────────────
    if tab_schema:
        for f in query.filters:
            op = f.operator if isinstance(f.operator, str) else f.operator.value

            if op in RANGE_OPERATORS:
                col_type = _get_col_type(f.field)

                if col_type == "empty":
                    raise ValidationError(
                        f"Column \"{f.field}\" has no data yet — "
                        "cannot apply a range filter on it."
                    )

                if col_type not in RANGE_OPERATOR_ALLOWED_TYPES:
                    raise ValidationError(
                        f"Cannot apply range comparison ('{op}') on "
                        f"\"{f.field}\" (type: {col_type or 'text'}). "
                        f"Use 'eq', 'contains', or 'in' instead."
                    )

    # ── 5. Aggregation validation ─────────────────────────────────────────────
    agg = query.aggregation if isinstance(query.aggregation, str) else query.aggregation.value

    if agg in ("sum", "average", "min", "max"):
        has_target   = bool(query.target_field)
        # Multi-field sum: parser puts both columns in display_fields instead
        # of picking one as target_field.  This is valid — the engine handles it.
        has_display  = bool(query.display_fields)

        if not has_target and not has_display:
            raise ValidationError(
                f"Aggregation '{agg}' requires a target column. "
                "Please specify which column to compute."
            )

    if agg == "percentage":
        if not query.numerator_field or not query.denominator_field:
            raise ValidationError(
                "Percentage calculation needs both a numerator column "
                "and a denominator column."
            )
        if tab_schema:
            for field_name in (query.numerator_field, query.denominator_field):
                if field_name:
                    col_type = _get_col_type(field_name)
                    if col_type not in AGGREGATABLE_TYPES:
                        raise ValidationError(
                            f"Percentage calculation: \"{field_name}\" must be "
                            f"numeric or currency (got: {col_type or 'text'})."
                        )


def _suggest_close(field: str, available: list[str]) -> str | None:
    """
    Return the closest available column name to the given field.
    Uses difflib first (handles typos), then falls back to substring match.
    """
    # 1. difflib fuzzy match
    lower_available = [c.lower() for c in available]
    close = get_close_matches(field.lower(), lower_available, n=1, cutoff=0.6)
    if close:
        idx = lower_available.index(close[0])
        return available[idx]

    # 2. Substring fallback
    fl = field.lower()
    for col in available:
        if fl in col.lower() or col.lower() in fl:
            return col

    return None
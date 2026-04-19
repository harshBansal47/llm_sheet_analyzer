



def _dedup_columns(columns: list[str]) -> list[str]:
    """
    Append _2, _3 ... to repeated column names.
    e.g. ['Address', 'Loan', 'Address', 'Loan'] 
      →  ['Address', 'Loan', 'Address_2', 'Loan_2']
    """
    seen: dict[str, int] = {}
    result = []
    for col in columns:
        if col not in seen:
            seen[col] = 1
            result.append(col)
        else:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
    return result
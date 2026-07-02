import polars as pl


def percentile_score(expr: pl.Expr) -> pl.Expr:
    n = pl.len()

    return (expr.rank(method="average") - 1) / (n - 1) * 100

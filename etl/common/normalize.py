import polars as pl


def normalize_auth(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        [
            pl.col("source_user").str.split("@").list.first().alias("user"),
            pl.col("source_user").str.split("@").list.last().alias("domain"),
            (pl.col("timestamp") // 86400).alias("day"),
        ]
    ).filter(pl.col("user").str.starts_with("U"))

import polars as pl

SECONDS_PER_DAY = 86400


def add_day(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns((pl.col("timestamp") // SECONDS_PER_DAY).alias("day"))


def status_metrics():
    return [
        pl.len().alias("authentications"),
        (pl.col("status") == "Success").sum().alias("success_logins"),
        (pl.col("status") == "Failure").sum().alias("failed_logins"),
    ]

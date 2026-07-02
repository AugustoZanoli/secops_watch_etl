from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR

HOURLY_FILE = SILVER_DIR / "facts" / "hourly_activity.parquet"

OUTPUT_FILE = GOLD_DIR / "login_timeline.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    hourly = pl.read_parquet(HOURLY_FILE)

    timeline = (
        hourly.group_by("day")
        .agg(
            [
                pl.sum("authentications").alias("authentications"),
                pl.sum("success_logins").alias("success_logins"),
                pl.sum("failed_logins").alias("failed_logins"),
                pl.when((pl.col("hour") < 8) | (pl.col("hour") >= 18))
                .then(pl.col("authentications"))
                .otherwise(0)
                .sum()
                .alias("after_hours_logins"),
            ]
        )
        .sort("day")
    )

    timeline.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Dias: {timeline.height}")
    print(f"Tempo: {elapsed:.2f}s")
    print(timeline.head())


if __name__ == "__main__":
    main()

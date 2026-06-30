import polars as pl

from etl.common.paths import SILVER_DIR

INPUT_FILE = SILVER_DIR / "events" / "redteam_events.parquet"
OUTPUT_FILE = SILVER_DIR / "events" / "daily_redteam_activity.parquet"


def main():

    daily = (
        pl.scan_parquet(INPUT_FILE)
        .group_by("day")
        .agg(
            [
                pl.len().alias("events"),
                pl.n_unique("user").alias("users"),
                pl.n_unique("destination_computer").alias("targets"),
            ]
        )
        .sort("day")
        .collect()
    )

    daily.write_parquet(OUTPUT_FILE)

    print(daily)


if __name__ == "__main__":
    main()

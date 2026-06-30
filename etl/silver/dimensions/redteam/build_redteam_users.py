import polars as pl

from etl.common.paths import SILVER_DIR

INPUT_FILE = SILVER_DIR / "events" / "redteam_events.parquet"
OUTPUT_FILE = SILVER_DIR / "events" / "redteam_users.parquet"


def main():

    users = (
        pl.scan_parquet(INPUT_FILE)
        .group_by("user")
        .agg(
            [
                pl.len().alias("events"),
                pl.min("timestamp").alias("first_event"),
                pl.max("timestamp").alias("last_event"),
                pl.n_unique("source_computer").alias("source_computers"),
                pl.n_unique("destination_computer").alias("target_computers"),
            ]
        )
        .sort("events", descending=True)
        .collect()
    )

    users.write_parquet(OUTPUT_FILE)

    print(users.head())


if __name__ == "__main__":
    main()

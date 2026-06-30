import polars as pl

from etl.common.paths import SILVER_DIR

INPUT_FILE = SILVER_DIR / "events" / "redteam_events.parquet"
OUTPUT_FILE = SILVER_DIR / "events" / "redteam_computers.parquet"


def main():

    source = (
        pl.scan_parquet(INPUT_FILE)
        .group_by("source_computer")
        .len()
        .rename(
            {
                "source_computer": "computer",
                "len": "source_events",
            }
        )
    )

    target = (
        pl.scan_parquet(INPUT_FILE)
        .group_by("destination_computer")
        .len()
        .rename(
            {
                "destination_computer": "computer",
                "len": "target_events",
            }
        )
    )

    computers = (
        source.join(target, on="computer", how="full", coalesce=True)
        .with_columns(
            [
                pl.col("source_events").fill_null(0),
                pl.col("target_events").fill_null(0),
            ]
        )
        .sort("source_events", descending=True)
        .collect()
    )

    computers.write_parquet(OUTPUT_FILE)

    print(computers.head())


if __name__ == "__main__":
    main()

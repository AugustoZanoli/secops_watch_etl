from pathlib import Path

import polars as pl

from etl.common.features import add_day

from etl.common.paths import BRONZE_DIR, SILVER_DIR

INPUT_FILE = BRONZE_DIR / "redteam.parquet"
OUTPUT_FILE = SILVER_DIR / "events" / "redteam_events.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    redteam = (
        add_day(pl.scan_parquet(INPUT_FILE))
        .with_columns(
            [
                pl.col("user").str.split("@").list.first().alias("user"),
                ((pl.col("timestamp") % 86400) // 3600).alias("hour"),
            ]
        )
        .with_columns(
            (pl.col("hour").is_between(8, 18, closed="left")).alias("business_hours")
        )
        .sort("timestamp")
        .collect(engine="streaming")
    )

    redteam.write_parquet(OUTPUT_FILE)

    print("=" * 60)
    print(f"Eventos: {redteam.height:,}")
    print(f"Usuários: {redteam['user'].n_unique()}")
    print(f"Dias: {redteam['day'].n_unique()}")
    print(redteam.head())


if __name__ == "__main__":
    main()

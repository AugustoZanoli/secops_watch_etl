import polars as pl

from etl.common.paths import SILVER_DIR

COMPUTERS_FILE = SILVER_DIR / "computers.parquet"
DAILY_FILE = SILVER_DIR / "daily_computer_activity.parquet"
USER_COMPUTERS_FILE = SILVER_DIR / "user_computers.parquet"
REDTEAM_FILE = SILVER_DIR / "redteam_events.parquet"


def main():

    computers = pl.read_parquet(COMPUTERS_FILE)

    daily = (
        pl.scan_parquet(DAILY_FILE)
        .group_by("computer")
        .agg(
            [
                pl.len().alias("active_days"),
                pl.mean("authentications").alias("avg_daily_authentications"),
                pl.max("authentications").alias("max_daily_authentications"),
            ]
        )
        .collect()
    )

    users = (
        pl.scan_parquet(USER_COMPUTERS_FILE)
        .group_by("computer")
        .len()
        .rename({"len": "unique_users"})
        .collect()
    )

    redteam_source = (
        pl.scan_parquet(REDTEAM_FILE)
        .group_by("source_computer")
        .len()
        .rename({"source_computer": "computer", "len": "redteam_source_events"})
        .collect()
    )

    redteam_target = (
        pl.scan_parquet(REDTEAM_FILE)
        .group_by("destination_computer")
        .len()
        .rename({"destination_computer": "computer", "len": "redteam_target_events"})
        .collect()
    )

    computers = (
        computers.join(daily, on="computer", how="left")
        .join(users, on="computer", how="left")
        .join(redteam_source, on="computer", how="left")
        .join(redteam_target, on="computer", how="left")
        .with_columns(
            [
                pl.col("unique_users").fill_null(0),
                pl.col("redteam_source_events").fill_null(0),
                pl.col("redteam_target_events").fill_null(0),
            ]
        )
    )

    computers.write_parquet(COMPUTERS_FILE)

    print(computers.head())


if __name__ == "__main__":
    main()

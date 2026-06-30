from pathlib import Path

import polars as pl

from etl.common.paths import SILVER_DIR

USERS_FILE = SILVER_DIR / "dimensions" / "users.parquet"
DAILY_USERS_FILE = SILVER_DIR / "facts" / "daily_user_activity.parquet"
USER_COMPUTERS_FILE = SILVER_DIR / "facts" / "user_computers.parquet"
REDTEAM_FILE = SILVER_DIR / "events" / "redteam_events.parquet"


def main():

    users = pl.read_parquet(USERS_FILE)

    daily = (
        pl.scan_parquet(DAILY_USERS_FILE)
        .group_by("user")
        .agg(
            [
                pl.len().alias("active_days"),
                pl.mean("authentications").alias("avg_daily_authentications"),
                pl.max("authentications").alias("max_daily_authentications"),
            ]
        )
        .collect()
    )

    computers = (
        pl.scan_parquet(USER_COMPUTERS_FILE)
        .group_by("user")
        .len()
        .rename({"len": "unique_computers"})
        .collect()
    )

    redteam = (
        pl.scan_parquet(REDTEAM_FILE)
        .group_by("user")
        .len()
        .rename({"len": "redteam_events"})
        .with_columns(pl.lit(True).alias("is_redteam_user"))
        .collect()
    )

    users = (
        users.join(daily, on="user", how="left")
        .join(computers, on="user", how="left")
        .join(redteam, on="user", how="left")
        .with_columns(
            [
                pl.col("redteam_events").fill_null(0),
                pl.col("is_redteam_user").fill_null(False),
                pl.col("unique_computers").fill_null(0),
            ]
        )
    )

    users.write_parquet(USERS_FILE)

    print(users.head())


if __name__ == "__main__":
    main()

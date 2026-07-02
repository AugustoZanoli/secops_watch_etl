from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR

COMPUTERS_FILE = SILVER_DIR / "dimensions" / "computers.parquet"

OUTPUT_FILE = GOLD_DIR / "top_computers.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    computers = (
        pl.scan_parquet(COMPUTERS_FILE)
        .select(
            [
                "computer",
                "total_authentications",
                "success_logins",
                "failed_logins",
                "unique_users",
                "active_days",
                "avg_daily_authentications",
                "max_daily_authentications",
                "redteam_source_events",
                "redteam_target_events",
            ]
        )
        .sort(
            by=[
                "redteam_target_events",
                "redteam_source_events",
                "total_authentications",
            ],
            descending=[True, True, True],
        )
        .limit(100)
        .collect()
    )

    computers.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Computadores: {computers.height}")
    print(f"Tempo: {elapsed:.2f}s")
    print(computers.head(10))


if __name__ == "__main__":
    main()

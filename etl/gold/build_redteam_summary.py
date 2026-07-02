from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR

USERS_FILE = SILVER_DIR / "dimensions" / "users.parquet"
COMPUTERS_FILE = SILVER_DIR / "dimensions" / "computers.parquet"

OUTPUT_FILE = GOLD_DIR / "redteam_summary.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    users = pl.scan_parquet(USERS_FILE)
    computers = pl.scan_parquet(COMPUTERS_FILE)

    summary = pl.DataFrame(
        {
            "total_redteam_events": [
                users.select(pl.sum("redteam_events")).collect().item()
            ],
            "affected_users": [
                users.filter(pl.col("redteam_events") > 0)
                .select(pl.len())
                .collect()
                .item()
            ],
            "source_computers": [
                computers.filter(pl.col("redteam_source_events") > 0)
                .select(pl.len())
                .collect()
                .item()
            ],
            "target_computers": [
                computers.filter(pl.col("redteam_target_events") > 0)
                .select(pl.len())
                .collect()
                .item()
            ],
        }
    )

    summary.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Tempo: {elapsed:.2f}s")
    print(summary)


if __name__ == "__main__":
    main()

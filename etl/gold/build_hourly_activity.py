from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR

INPUT_FILE = SILVER_DIR / "facts" / "hourly_activity.parquet"
OUTPUT_FILE = GOLD_DIR / "hourly_activity.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    activity = pl.scan_parquet(INPUT_FILE).sort(["day", "hour"]).collect()

    activity.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Registros: {activity.height}")
    print(f"Tempo: {elapsed:.2f}s")
    print(activity.head())


if __name__ == "__main__":
    main()

from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR

USER_RISK_FILE = GOLD_DIR / "user_risk.parquet"

OUTPUT_FILE = GOLD_DIR / "risk_distribution.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    distribution = (
        pl.scan_parquet(USER_RISK_FILE)
        .group_by("risk_level")
        .len()
        .rename({"len": "users"})
        .with_columns(
            pl.col("risk_level")
            .replace_strict(
                {
                    "Critical": 0,
                    "High": 1,
                    "Medium": 2,
                    "Low": 3,
                }
            )
            .alias("order")
        )
        .sort("order")
        .drop("order")
        .collect()
    )

    distribution.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Níveis: {distribution.height}")
    print(f"Tempo: {elapsed:.2f}s")
    print(distribution)


if __name__ == "__main__":
    main()

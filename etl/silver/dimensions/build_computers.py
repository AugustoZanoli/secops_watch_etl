from time import perf_counter
import shutil

import polars as pl

from etl.common.normalize import normalize_auth
from etl.common.paths import BRONZE_DIR, SILVER_DIR

BRONZE_AUTH_DIR = BRONZE_DIR / "auth"

OUTPUT_FILE = SILVER_DIR / "computers.parquet"
TMP_DIR = SILVER_DIR / "tmp" / "computers"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def reset_tmp():
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)

    TMP_DIR.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    reset_tmp()

    files = sorted(BRONZE_AUTH_DIR.glob("*.parquet"))

    for i, file in enumerate(files):

        print(f"[{i+1}/{len(files)}] {file.name}")

        chunk = (
            normalize_auth(pl.scan_parquet(file))
            .group_by("destination_computer")
            .agg(
                [
                    pl.len().alias("total_authentications"),
                    (pl.col("status") == "Success").sum().alias("success_logins"),
                    (pl.col("status") == "Failure").sum().alias("failed_logins"),
                    pl.min("timestamp").alias("first_seen"),
                    pl.max("timestamp").alias("last_seen"),
                ]
            )
            .collect(engine="streaming")
        )

        chunk.write_parquet(TMP_DIR / f"part-{i:05}.parquet")

    print("\nMesclando resultados...")

    computers = (
        pl.scan_parquet(str(TMP_DIR / "*.parquet"))
        .filter(pl.col("destination_computer").str.starts_with("C"))
        .group_by("destination_computer")
        .agg(
            [
                pl.sum("total_authentications").alias("total_authentications"),
                pl.sum("success_logins").alias("success_logins"),
                pl.sum("failed_logins").alias("failed_logins"),
                pl.min("first_seen").alias("first_seen"),
                pl.max("last_seen").alias("last_seen"),
            ]
        )
        .rename({"destination_computer": "computer"})
        .collect(engine="streaming")
    )

    computers.write_parquet(OUTPUT_FILE)

    shutil.rmtree(TMP_DIR)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print(f"Computadores: {computers.height:,}")
    print(f"Tempo: {elapsed:.2f}s")
    print(computers.head())


if __name__ == "__main__":
    main()

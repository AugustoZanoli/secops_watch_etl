from pathlib import Path
from time import perf_counter

import polars as pl
import shutil

from etl.common.normalize import normalize_auth
from etl.common.paths import BRONZE_DIR, SILVER_DIR

BRONZE_AUTH_DIR = BRONZE_DIR / "auth"
OUTPUT_FILE = SILVER_DIR / "dimensions" / "users.parquet"
TMP_DIR = SILVER_DIR / "tmp" / "users"

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
            .filter(pl.col("user").str.starts_with("U"))
            .group_by("user")
            .agg(
                [
                    pl.len().alias("total_logins"),
                    (pl.col("status") == "Success").sum().alias("success_logins"),
                    (pl.col("status") == "Failure").sum().alias("failed_logins"),
                    pl.min("timestamp").alias("first_login"),
                    pl.max("timestamp").alias("last_login"),
                ]
            )
            .collect(engine="streaming")
        )

        chunk.write_parquet(TMP_DIR / f"part-{i:05}.parquet")

    print("\nMesclando resultados...")

    users = (
        pl.scan_parquet(str(TMP_DIR / "*.parquet"))
        .group_by("user")
        .agg(
            [
                pl.sum("total_logins").alias("total_logins"),
                pl.sum("success_logins").alias("success_logins"),
                pl.sum("failed_logins").alias("failed_logins"),
                pl.min("first_login").alias("first_login"),
                pl.max("last_login").alias("last_login"),
            ]
        )
        .collect(engine="streaming")
    )

    users.write_parquet(OUTPUT_FILE)

    shutil.rmtree(TMP_DIR)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print(f"Usuários: {users.height:,}")
    print(f"Tempo: {elapsed:.2f}s")


if __name__ == "__main__":
    main()

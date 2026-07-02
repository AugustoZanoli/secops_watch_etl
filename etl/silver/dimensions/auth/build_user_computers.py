from time import perf_counter
import shutil

import polars as pl

from etl.common.normalize import normalize_auth
from etl.common.paths import BRONZE_DIR, SILVER_DIR

BRONZE_AUTH_DIR = BRONZE_DIR / "auth"

OUTPUT_FILE = SILVER_DIR / "facts" / "user_computers.parquet"
TMP_DIR = SILVER_DIR / "tmp" / "user_computers"

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
            .select(["user", pl.col("destination_computer").alias("computer")])
            .filter(pl.col("computer").str.starts_with("C"))
            .unique()
            .collect(engine="streaming")
        )

        chunk.write_parquet(TMP_DIR / f"part-{i:05}.parquet")

    print("\nMesclando resultados...")

    user_computers = (
        pl.scan_parquet(str(TMP_DIR / "*.parquet")).unique().collect(engine="streaming")
    )

    user_computers.write_parquet(OUTPUT_FILE)

    shutil.rmtree(TMP_DIR)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print(f"Relacionamentos: {user_computers.height:,}")
    print(f"Tempo: {elapsed:.2f}s")
    print(user_computers.head())


if __name__ == "__main__":
    main()

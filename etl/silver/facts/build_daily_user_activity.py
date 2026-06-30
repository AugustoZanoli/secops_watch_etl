from time import perf_counter
import shutil

import polars as pl

from etl.common.features import add_day, status_metrics
from etl.common.normalize import normalize_auth
from etl.common.paths import BRONZE_DIR, SILVER_DIR

BRONZE_AUTH_DIR = BRONZE_DIR / "auth"

OUTPUT_FILE = SILVER_DIR / "daily_user_activity.parquet"
TMP_DIR = SILVER_DIR / "tmp" / "daily_user_activity"

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
            add_day(normalize_auth(pl.scan_parquet(file)))
            .group_by(["day", "user"])
            .agg(status_metrics())
            .collect(engine="streaming")
        )

        chunk.write_parquet(TMP_DIR / f"part-{i:05}.parquet")

    print("\nMesclando resultados...")

    daily_users = (
        pl.scan_parquet(str(TMP_DIR / "*.parquet"))
        .group_by(["day", "user"])
        .agg(
            [
                pl.sum("authentications").alias("authentications"),
                pl.sum("success_logins").alias("success_logins"),
                pl.sum("failed_logins").alias("failed_logins"),
            ]
        )
        .sort(["day", "user"])
        .collect(engine="streaming")
    )

    daily_users.write_parquet(OUTPUT_FILE)

    shutil.rmtree(TMP_DIR)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print(f"Registros: {daily_users.height:,}")
    print(f"Usuários: {daily_users['user'].n_unique():,}")
    print(f"Dias: {daily_users['day'].n_unique()}")
    print(f"Tempo: {elapsed:.2f}s")
    print(daily_users.head())


if __name__ == "__main__":
    main()

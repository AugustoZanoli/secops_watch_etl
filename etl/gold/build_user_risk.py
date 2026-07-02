from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR
from etl.common.scoring import percentile_score

USERS_FILE = SILVER_DIR / "dimensions" / "users.parquet"
OUTPUT_FILE = GOLD_DIR / "user_risk.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    users = pl.read_parquet(USERS_FILE)

    users = users.with_columns(
        [
            percentile_score(pl.col("total_logins")).alias("login_score"),
            percentile_score(pl.col("unique_computers")).alias("computer_score"),
            percentile_score(pl.col("max_daily_authentications")).alias("volume_score"),
            percentile_score(pl.col("redteam_events")).alias("redteam_score"),
        ]
    )

    users = users.with_columns(
        (
            pl.col("login_score") * 0.20
            + pl.col("computer_score") * 0.25
            + pl.col("volume_score") * 0.30
            + pl.col("redteam_score") * 0.25
        )
        .round(2)
        .alias("risk_score")
    )

    users = users.with_columns(
        pl.when(pl.col("risk_score") >= 80)
        .then(pl.lit("Critical"))
        .when(pl.col("risk_score") >= 65)
        .then(pl.lit("High"))
        .when(pl.col("risk_score") >= 40)
        .then(pl.lit("Medium"))
        .otherwise(pl.lit("Low"))
        .alias("risk_level")
    )

    result = users.select(
        [
            "user",
            "risk_score",
            "risk_level",
            "login_score",
            "computer_score",
            "volume_score",
            "redteam_score",
        ]
    ).sort("risk_score", descending=True)

    result.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Usuários: {result.height:,}")
    print(f"Tempo: {elapsed:.2f}s")
    print(result.head(10))


if __name__ == "__main__":
    main()

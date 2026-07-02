from time import perf_counter

import polars as pl

from etl.common.paths import GOLD_DIR, SILVER_DIR

USERS_FILE = SILVER_DIR / "dimensions" / "users.parquet"
USER_RISK_FILE = GOLD_DIR / "user_risk.parquet"

OUTPUT_FILE = GOLD_DIR / "top_users.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    users = pl.scan_parquet(USERS_FILE)
    risk = pl.scan_parquet(USER_RISK_FILE)

    top_users = (
        users.join(risk, on="user", how="inner")
        .select(
            [
                "user",
                "risk_score",
                "risk_level",
                "redteam_events",
                "total_logins",
                "unique_computers",
                "active_days",
                "max_daily_authentications",
            ]
        )
        .sort("risk_score", descending=True)
        .limit(100)
        .collect()
    )

    top_users.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Usuários: {top_users.height}")
    print(f"Tempo: {elapsed:.2f}s")
    print(top_users.head(10))


if __name__ == "__main__":
    main()

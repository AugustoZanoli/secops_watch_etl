from time import perf_counter

import polars as pl

from etl.common.paths import SILVER_DIR, GOLD_DIR

USERS_FILE = SILVER_DIR / "dimensions" / "users.parquet"
HOURLY_FILE = SILVER_DIR / "facts" / "hourly_activity.parquet"
USER_RISK_FILE = GOLD_DIR / "user_risk.parquet"

OUTPUT_FILE = GOLD_DIR / "dashboard_kpis.parquet"

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():

    start = perf_counter()

    users = pl.read_parquet(USERS_FILE)
    hourly = pl.read_parquet(HOURLY_FILE)
    risk = pl.read_parquet(USER_RISK_FILE)

    total_users = users.height

    suspicious_users = risk.filter(pl.col("risk_score") >= 65).height

    average_risk = risk["risk_score"].mean()

    critical_users = risk.filter(pl.col("risk_level") == "Critical").height

    high_users = risk.filter(pl.col("risk_level") == "High").height

    medium_users = risk.filter(pl.col("risk_level") == "Medium").height

    low_users = risk.filter(pl.col("risk_level") == "Low").height

    total_auth = hourly["authentications"].sum()
    failed_auth = hourly["failed_logins"].sum()

    authentication_failure_rate = (
        0.0 if total_auth == 0 else failed_auth / total_auth * 100
    )

    after_hours_logins = hourly.filter((pl.col("hour") < 8) | (pl.col("hour") >= 18))[
        "authentications"
    ].sum()

    kpis = pl.DataFrame(
        {
            "total_users": [total_users],
            "suspicious_users": [suspicious_users],
            "average_risk": [round(average_risk, 2)],
            "authentication_failure_rate": [round(authentication_failure_rate, 2)],
            "after_hours_logins": [after_hours_logins],
            "critical_users": [critical_users],
            "high_users": [high_users],
            "medium_users": [medium_users],
            "low_users": [low_users],
        }
    )

    kpis.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Tempo: {elapsed:.2f}s")
    print(kpis)


if __name__ == "__main__":
    main()

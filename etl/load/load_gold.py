from time import perf_counter

import polars as pl

from etl.common.config.database import engine
from etl.common.paths import GOLD_DIR

TABLES = [
    "user_risk",
    "dashboard_kpis",
    "login_timeline",
    "risk_distribution",
    "top_users",
    "top_computers",
    "hourly_activity",
    "redteam_summary",
]


def main():

    start = perf_counter()

    with engine.begin() as conn:

        for table in TABLES:

            df = pl.read_parquet(GOLD_DIR / f"{table}.parquet")

            print(f"Carregando {table} ({df.height:,} registros)...")

            conn.exec_driver_sql(f"TRUNCATE TABLE {table}")

            df.write_database(
                table_name=table,
                connection=conn,
                if_table_exists="append",
            )

    elapsed = perf_counter() - start

    print("=" * 60)
    print(f"Tabelas carregadas: {len(TABLES)}")
    print(f"Tempo: {elapsed:.2f}s")


if __name__ == "__main__":
    main()

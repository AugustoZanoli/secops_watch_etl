"""
Orquestrador do pipeline ETL: bronze -> silver (dimensoes/eventos/facts/features) -> gold -> load opcional.
"""

import argparse
from time import perf_counter

from etl.bronze import ingest_auth, ingest_redteam

from etl.silver.dimensions.auth import build_computers as silver_build_computers
from etl.silver.dimensions.auth import build_user_computers as silver_build_user_computers
from etl.silver.dimensions.auth import build_users as silver_build_users
from etl.silver.dimensions.redteam import (
    build_daily_redteam_activity,
    build_redteam_computers,
    build_redteam_users,
)
from etl.silver.events import build_redteam_events
from etl.silver.facts import build_daily_activity as fact_build_daily_activity
from etl.silver.facts import build_daily_computer_activity
from etl.silver.facts import build_daily_user_activity
from etl.silver.facts import build_hourly_activity as fact_build_hourly_activity
from etl.silver.features import build_computer_features, build_user_features

from etl.gold import build_dashboard_kpis
from etl.gold import build_hourly_activity as gold_build_hourly_activity
from etl.gold import (
    build_login_timeline,
    build_redteam_summary,
    build_risk_distribution,
    build_top_computers,
    build_top_users,
    build_user_risk,
)

from etl.load import load_gold

STAGES = [
    (
        "Bronze - Aquisicao dos dados",
        [
            ("Ingestao auth", ingest_auth),
            ("Ingestao redteam", ingest_redteam),
        ],
    ),
    (
        "Silver - Dimensoes e eventos base",
        [
            ("Dimensao users", silver_build_users),
            ("Dimensao computers", silver_build_computers),
            ("Bridge user <-> computer", silver_build_user_computers),
            ("Eventos redteam", build_redteam_events),
        ],
    ),
    (
        "Silver - Dimensoes derivadas de redteam",
        [
            ("Redteam por usuario", build_redteam_users),
            ("Redteam por computador", build_redteam_computers),
            ("Atividade diaria redteam", build_daily_redteam_activity),
        ],
    ),
    (
        "Silver - Facts de atividade",
        [
            ("Atividade diaria geral", fact_build_daily_activity),
            ("Atividade diaria por computador", build_daily_computer_activity),
            ("Atividade diaria por usuario", build_daily_user_activity),
            ("Atividade horaria", fact_build_hourly_activity),
        ],
    ),
    (
        "Silver - Features (enriquecimento das dimensoes)",
        [
            ("Features de usuario", build_user_features),
            ("Features de computador", build_computer_features),
        ],
    ),
    (
        "Gold - Metricas analiticas",
        [
            ("Score de risco por usuario", build_user_risk),
            ("Atividade horaria (gold)", gold_build_hourly_activity),
            ("Linha do tempo de logins", build_login_timeline),
            ("Resumo redteam", build_redteam_summary),
            ("Distribuicao de risco", build_risk_distribution),
            ("Top computadores", build_top_computers),
            ("Top usuarios", build_top_users),
            ("KPIs do dashboard", build_dashboard_kpis),
        ],
    ),
]

LOAD_STAGE = ("Load - Carga no Postgres", [("Carga das tabelas gold", load_gold)])


def run_stage(name, steps) -> None:
    print()
    print("#" * 60)
    print(f"# {name}")
    print("#" * 60)

    for step_name, module in steps:
        print(f"\n>>> {step_name} ({module.__name__})")
        module.main()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executa o pipeline ETL completo (bronze -> silver -> gold)."
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Pula a ingestao do bronze e reaproveita os parquets ja existentes.",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Ao final, carrega as tabelas gold no Postgres (requer schema ja criado no banco).",
    )
    parser.add_argument(
        "--only-load",
        action="store_true",
        help="Executa somente a carga no Postgres, pulando bronze/silver/gold.",
    )
    args = parser.parse_args()

    start = perf_counter()

    stages = [] if args.only_load else STAGES[1:] if args.skip_bronze else STAGES

    for name, steps in stages:
        run_stage(name, steps)

    if args.load or args.only_load:
        run_stage(*LOAD_STAGE)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print(f"Pipeline concluido em {elapsed:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()

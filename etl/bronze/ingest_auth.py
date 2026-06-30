from pathlib import Path
from time import perf_counter
import shutil

import pandas as pd
import polars as pl
from tqdm import tqdm

# =====================================
# Configurações
# =====================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "auth.txt.gz"
OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze" / "auth"

CHUNK_SIZE = 500_000

COLUMNS = [
    "timestamp",
    "source_user",
    "destination_user",
    "source_computer",
    "destination_computer",
    "authentication_type",
    "logon_type",
    "authentication_orientation",
    "status",
]


# =====================================
# Utilidades
# =====================================


def prepare_output_directory() -> None:
    """
    Remove a Bronze antiga (caso exista) e cria uma nova.
    """
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =====================================
# ETL
# =====================================


def ingest_auth() -> None:
    print("=" * 60)
    print("INGESTÃO - AUTH DATASET")
    print("=" * 60)

    prepare_output_directory()

    start = perf_counter()

    total_rows = 0
    total_parts = 0

    reader = pd.read_csv(
        RAW_FILE, header=None, names=COLUMNS, chunksize=CHUNK_SIZE, compression="gzip"
    )

    for part_number, chunk in enumerate(
        tqdm(reader, desc="Criando Bronze", unit="chunk")
    ):
        pl.from_pandas(chunk).write_parquet(
            OUTPUT_DIR / f"part-{part_number:05}.parquet"
        )

        total_rows += len(chunk)
        total_parts += 1

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print("Bronze criada com sucesso!")
    print("=" * 60)
    print(f"Arquivos gerados : {total_parts}")
    print(f"Total de linhas  : {total_rows:,}")
    print(f"Tempo            : {elapsed:.2f}s")
    print(f"Destino          : {OUTPUT_DIR}")


def validate_dataset() -> None:
    """
    Faz uma validação simples do dataset gerado.
    """
    print("\nValidando Bronze...")

    auth = pl.scan_parquet(str(OUTPUT_DIR / "*.parquet"))

    summary = auth.select(
        [
            pl.len().alias("rows"),
            pl.n_unique("source_user").alias("source_users"),
            pl.n_unique("destination_user").alias("destination_users"),
            pl.n_unique("source_computer").alias("source_computers"),
            pl.n_unique("destination_computer").alias("destination_computers"),
        ]
    ).collect(engine="streaming")

    print(summary)


# =====================================
# Main
# =====================================


def main():
    ingest_auth()
    validate_dataset()


if __name__ == "__main__":
    main()

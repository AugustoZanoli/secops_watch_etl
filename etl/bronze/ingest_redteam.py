from pathlib import Path
from time import perf_counter

import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "redteam.txt.gz"
OUTPUT_FILE = PROJECT_ROOT / "data" / "bronze" / "redteam.parquet"

COLUMNS = [
    "timestamp",
    "user",
    "source_computer",
    "destination_computer",
]


def ingest_redteam() -> None:
    print("=" * 60)
    print("INGESTÃO - REDTEAM DATASET")
    print("=" * 60)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    start = perf_counter()

    raw = pd.read_csv(RAW_FILE, header=None, names=COLUMNS, compression="gzip")

    redteam = pl.from_pandas(raw)
    redteam.write_parquet(OUTPUT_FILE)

    elapsed = perf_counter() - start

    print()
    print("=" * 60)
    print("Bronze criada com sucesso!")
    print("=" * 60)
    print(f"Total de linhas : {redteam.height:,}")
    print(f"Tempo           : {elapsed:.2f}s")
    print(f"Destino         : {OUTPUT_FILE}")


def validate_dataset() -> None:
    print("\nValidando Bronze...")

    redteam = pl.scan_parquet(OUTPUT_FILE)

    summary = redteam.select(
        [
            pl.len().alias("rows"),
            pl.n_unique("user").alias("users"),
            pl.n_unique("source_computer").alias("source_computers"),
            pl.n_unique("destination_computer").alias("destination_computers"),
        ]
    ).collect()

    print(summary)


def main():
    ingest_redteam()
    validate_dataset()


if __name__ == "__main__":
    main()

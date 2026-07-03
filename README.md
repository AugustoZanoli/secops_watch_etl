# Login Anomaly Dashboard — ETL

Pipeline ETL que processa os datasets de autenticação (`auth`) e atividade de red team (`redteam`) do LANL Unified Host and Network Dataset, transformando-os em camadas **bronze -> silver -> gold** para alimentar um dashboard de detecção de anomalias de login.

## Arquitetura

```
data/
  raw/      arquivos originais (.txt.gz)
  bronze/   parquet bruto, particionado (auth) ou único (redteam)
  silver/   dimensões, eventos, facts e features derivados do bronze
  gold/     métricas analíticas prontas para consumo (KPIs, risk score, timelines)
```

- **Bronze** (`etl/bronze/`): ingestão dos `.txt.gz` em parquet. O dataset `auth` é lido em chunks (~500k linhas) e gravado em ~2100 arquivos parquet; `redteam` é pequeno e gravado em um único arquivo.
- **Silver** (`etl/silver/`): dimensões (`users`, `computers`, `user_computers`, dimensões derivadas de redteam), eventos, facts de atividade diária/horária e features de enriquecimento.
- **Gold** (`etl/gold/`): score de risco por usuário, distribuição de risco, top usuários/computadores, timeline de login, resumo redteam e KPIs do dashboard.
- **Load** (`etl/load/`): carga opcional das tabelas gold no Postgres (via `sqlalchemy`), truncando e reinserindo cada tabela.

Utilitários comuns (paths, normalização, scoring, features compartilhadas) ficam em `etl/common/`.

## Requisitos

- Python 3.14+
- Dependências em `requirements.txt` (`polars`, `pandas`, `tqdm`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`)
- Para a etapa de load: Postgres com o schema já criado e `DATABASE_URL` configurada (ver `.env.example`)

```bash
python -m venv .venv
.venv/Scripts/activate  # Windows
pip install -r requirements.txt
```

Os arquivos brutos (`auth.txt.gz`, `redteam.txt.gz`) devem estar em `data/raw/` antes de rodar o pipeline.

## Uso

Rodar o pipeline completo (bronze → silver → gold):

```bash
python -m etl.run_pipeline
```

Opções:

| Flag            | Efeito                                                            |
| --------------- | ----------------------------------------------------------------- |
| `--skip-bronze` | Pula a ingestão do bronze e reaproveita os parquets já existentes |
| `--load`        | Ao final, carrega as tabelas gold no Postgres                     |
| `--only-load`   | Executa somente a carga no Postgres, pulando bronze/silver/gold   |

Cada etapa também pode ser executada individualmente como módulo (ex: `python -m etl.bronze.ingest_auth`), útil para depurar uma camada específica sem rodar o pipeline inteiro.

## Notas

- O dataset `auth` tem mais de 1 bilhão de linhas distribuídas em ~2100 arquivos parquet na camada bronze; evite agregações de alta cardinalidade (`n_unique` por coluna) sobre o dataset inteiro. Isso já causou estouro de memória em execuções anteriores. A validação da camada bronze foi reduzida a uma contagem de linhas (`etl/bronze/ingest_auth.py`).
- O pipeline completo (bronze + silver + gold) leva cerca de **40 minutos**.

# ETL Mini Pipeline — Synthetic Transactions

A small data engineering project. It generates **synthetic transaction data** as daily CSV batches and is structured to grow into a full ETL/ELT: transform + validation, loading into a warehouse, and orchestration with Airflow.

## Features
- Generates **N transactions per day** (default **1000**).
- **ISO timestamp** (`YYYY-MM-DDTHH:MM:SS`), 2-decimal amounts.
- Currencies limited to `{USD, EUR, HUF}`.
- ~**5%** `is_chargeback` flag (0/1).
- Deterministic IDs: `txn_000001 … txn_00NNNN` (seeded randomness).
- **CLI**:
  - `--run-date` (or `--run_date`) for the target day
  - `-n` / `--n-records` for the number of rows
- Output file: `data/raw/transactions_<YYYY-MM-DD>.csv`

## Quickstart

```bash
# 1) Create & activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\Activate.ps1

# 2) Install dependencies
pip install -r requirements.txt

# 3) Generate data (defaults: today, 1000 rows)
python src/generate/generate_transactions.py

# Generate for a specific day with custom size
python src/generate/generate_transactions.py --run-date 2025-09-01 -n 500

Project structure

src/
  generate/
    generate_transactions.py   # data generator with CLI
data/
  raw/                         # generated CSVs (gitignored)
README.md
requirements.txt






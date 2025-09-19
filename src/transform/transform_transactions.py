# IMPORTS

import pandera.pandas as pa
from pandera import Column, Check
import argparse
import pathlib
import pandas as pd
from datetime import date

# CONSTANTS
RAW_DIR = "data/raw"
CURATED_DIR = "data/curated"
QUARANTINE_DIR = "data/quarantine"

RAW_TEMPLATE = "transactions_{}.csv"
CURATED_TEMPLATE = "transactions_curated_{}.csv"
QUARANTINE_TEMPLATE = "transactions_quarantine_{}.csv"

# ARGPARSE

def parse_args():
    parser = argparse.ArgumentParser(description="Transform raw → curated with data validation (Pandera).")
    parser.add_argument(
        "--run-date", "--run_date",
        dest="run_date",
        type=str, 
        default=None, 
        help="Date for which to transform data in 'YYYY-MM-DD' format. Defaults to today's date.")

    args = parser.parse_args()
    return args

# DATA TRANSFORMATION

def main():
    args = parse_args()

    # date handling
    if args.run_date:
        try:
            today_obj = date.fromisoformat(args.run_date)
        except ValueError:
            raise SystemExit("Error: run_date must be in 'YYYY-MM-DD' format.")
    else:
        today_obj = date.today()
    today_str = today_obj.isoformat()

    # pathlib paths
    raw_dir = pathlib.Path(RAW_DIR)
    curated_dir = pathlib.Path(CURATED_DIR); curated_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir = pathlib.Path(QUARANTINE_DIR); quarantine_dir.mkdir(parents=True, exist_ok=True)

    input_file = raw_dir / RAW_TEMPLATE.format(today_str)
    curated_file = curated_dir / CURATED_TEMPLATE.format(today_str)
    quarantine_file = quarantine_dir / QUARANTINE_TEMPLATE.format(today_str)

    # read raw data
    try:
        df_raw = pd.read_csv(input_file)
    except FileNotFoundError:
        raise SystemExit(f"Error: Input file {input_file} not found.")

    # Pandera schema validation
    schema = pa.DataFrameSchema({
        "transaction_id": Column(pa.String, checks=Check.str_matches(r"^txn_\d{6}$"), unique=True),
        "user_id": Column(pa.String, checks=Check.str_matches(r"^u_\d{4}$")),
        "timestamp": Column(pa.DateTime, coerce=True),
        "amount": Column(pa.Float, checks=Check.gt(0)),
        "currency": Column(pa.String, checks=Check.isin(["USD", "EUR", "HUF"])),
        "payment_method": Column(pa.String, checks=Check.isin(["credit_card", "paypal", "debit_card", "bank_transfer", "crypto"])),
        "country": Column(pa.String, checks=[Check.str_length(2,2), Check.str_matches(r"^[A-Z]{2}$")]),
        "device": Column(pa.String, checks=Check.isin(["mobile", "desktop", "tablet"])),
        "is_chargeback": Column(pa.Int, checks=Check.isin([0, 1]))
    }, coerce=True, strict=True)

    # validate and separate good vs bad data
    try:
        df_validated = schema.validate(df_raw, lazy=True)
        df_good = df_validated
        df_bad = pd.DataFrame(columns = df_raw.columns) # empty df
    except pa.errors.SchemaErrors as e:
        print("Data validation errors found. Moving invalid data to quarantine.")
        failures = e.failure_cases
        bad_indices = sorted(set(failures["index"]))
        df_bad = df_raw.loc[bad_indices].copy()
        df_good = df_raw.drop(index=bad_indices).copy()

    # save curated and quarantine data

    df_good.to_csv(curated_file, index=False)
    if not df_bad.empty:
        df_bad.to_csv(quarantine_file, index=False)

    # print summary
    total_records = len(df_raw)
    good_records = len(df_good)
    bad_records = len(df_bad)

    print(f"Total records processed: {total_records}")
    if total_records > 0:
        print(f"Valid records: {good_records} ({(good_records/total_records):.2%})")
        print(f"Invalid records: {bad_records} ({(bad_records/total_records):.2%})")
    if bad_records > 0:
        print(f"Invalid records moved to quarantine: {quarantine_file}")
    else:
        print("No invalid records. Quarantine file not written.")
    print(f"Transformed data saved to {curated_file}")

if __name__ == "__main__":
    main()
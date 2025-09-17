# IMPORTS
import pandas as pd
from datetime import date, datetime, time, timedelta
import random
import pathlib
from faker import Faker
import argparse

# SEEDING
SEED = 42

random.seed(SEED)

fake = Faker()
fake.seed_instance(SEED)

# CONSTANTS
N_RECORDS = 1000
OUTPUT_DIR = "data/raw"
CURRENCIES = ['USD', 'EUR', 'HUF']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
DEVICES = ['mobile', 'desktop', 'tablet']
FILE_TEMPLATE = "transactions_{}.csv"

CHARGEBACK_RATE = 0.05  # 5% chargeback rate
AMOUNT_MIN, AMOUNT_MAX = 1.0, 1000.0

N_USERS = 300   # Number of unique users

# HELPER FUNCTIONS

def generate_transaction_id (i: int, prefix: str = "txn", width: int = 6 ) -> str:
    """Generate a unique transaction ID."""
    return f"{prefix}_{i:0{width}d}"

def choose_user_id (user_pool: list[str])  -> str:
    """
    Choose a random user_id from the user_pool.
    """
    return random.choice(user_pool) 

def generate_timestamp(today: date) -> str:
    """
    Generate a random transaction date within the last year. Format: 'YYYY-MM-DDTHH:MM:SS'. 
    """
    start_day = today - timedelta(days=365)
    start_dt = datetime.combine(start_day, time.min)
    end_dt = datetime.combine(today, time.max)
    dt = fake.date_time_between(start_date=start_dt, end_date=end_dt)
    return dt.isoformat(timespec='seconds') # 'YYYY-MM-DDTHH:MM:SS'

def generate_amount(min_val=AMOUNT_MIN, max_val=AMOUNT_MAX) -> float:
    """
    Generate a random transaction amount between min_val and max_val."""
    return round(random.uniform(min_val, max_val), 2)

def generate_currency() -> str:
    """Generate a random currency from the CURRENCIES list."""
    return random.choice(CURRENCIES)    

def generate_payment_method():
    return random.choice(PAYMENT_METHODS)

def generate_country():
    return fake.country_code()  

def generate_device():
    return random.choice(DEVICES)   

def generate_is_chargeback() -> int:
    return int(random.random() < CHARGEBACK_RATE)

# PARSE ARGS

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data.")
    parser.add_argument(
        "--run-date", "--run_date",
        dest="run_date",
        type=str, 
        default=None, 
        help="Date for which to generate data in 'YYYY-MM-DD' format. Defaults to today's date.")
    
    parser.add_argument(
        "-n", "--n-records",
        dest="n_records",
        type=int,
        default=N_RECORDS, 
        help= "Number of transaction records to generate. Default is 1000.")
    args = parser.parse_args()
    return args

# DATA GENERATION

def main():
    args = parse_args()

    if args.n_records <= 0:
        raise SystemExit("Error: --n-records must be a positive integer.")

    if args.run_date:
        try:
            today_obj = date.fromisoformat(args.run_date)
        except ValueError:
            raise SystemExit("Error: run_date must be in 'YYYY-MM-DD' format.")
    else:
        today_obj = date.today()
    today_str = today_obj.isoformat()
    
    output_dir = pathlib.Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / FILE_TEMPLATE.format(today_str)

    user_pool = [f"u_{i:04d}" for i in range (1, N_USERS + 1)] 

    transactions = []
    for i in range(1, args.n_records + 1):
        ts = generate_timestamp(today_obj)
        transaction = {
            "transaction_id": generate_transaction_id(i),
            "user_id": choose_user_id(user_pool),
            "timestamp": ts,
            "amount": generate_amount(),
            "currency": generate_currency(),
            "payment_method": generate_payment_method(),
            "country": generate_country(),
            "device": generate_device(),
            "is_chargeback": generate_is_chargeback()
        }
        transactions.append(transaction)

    cols = ["transaction_id", "user_id", "timestamp", "amount", "currency", 
            "payment_method", "country", "device", "is_chargeback"]
    
    df = pd.DataFrame(transactions, columns=cols)
    df.to_csv(output_file, index=False)

    print(f"Generated {args.n_records} transactions and saved to {output_file}")
    print(f"Length of records generated: {len(df)}")
    print(f"Chargeback rate: {df['is_chargeback'].mean():.2%}")
    print(df.head(3))

if __name__ == "__main__":
    main()




import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path


# -----------------------------
# Reproducibility
# -----------------------------
SEED = 42
BASE_DATE = datetime(2026, 7, 21)

random.seed(SEED)
np.random.seed(SEED)
Faker.seed(SEED)

fake = Faker()
fake.seed_instance(SEED)


# -----------------------------
# Output location
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Config
# -----------------------------
NUM_TRANSACTIONS = 10000
NUM_CUSTOMERS = 1500

card_networks = ["Visa", "Mastercard", "Pulse"]
merchant_categories = ["Retail", "Travel", "Food", "Electronics", "Gas", "Online"]
transaction_statuses = ["Approved", "Declined", "Reversed"]
dispute_reasons = ["Fraud", "Product Not Received", "Duplicate Charge", "Incorrect Amount"]
dispute_statuses = ["Opened", "Under Review", "Resolved", "Denied"]
chargeback_outcomes = ["Won", "Lost", "Pending"]


def random_date(days_back=365):
    """
    Generate a deterministic fake datetime within a fixed window.

    BASE_DATE is fixed so the generated dataset does not change depending
    on the day the script is run.
    """
    end_date = BASE_DATE
    start_date = end_date - timedelta(days=days_back)
    return fake.date_time_between(start_date=start_date, end_date=end_date)


# -----------------------------
# Customers
# -----------------------------
customers = []

for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        "customer_id": f"CUST_{i:06d}",
        "account_id": f"ACCT_{i:06d}",
        "customer_age": random.randint(18, 75),
        "account_status": random.choice(["Active", "Suspended", "Closed"]),
        "state": fake.state_abbr(),
        "created_at": random_date(1000).strftime("%Y-%m-%d")
    })

customers_df = pd.DataFrame(customers)


# -----------------------------
# Transactions
# -----------------------------
transactions = []

for i in range(1, NUM_TRANSACTIONS + 1):
    customer = customers_df.sample(1).iloc[0]
    transaction_timestamp = random_date(365)

    transactions.append({
        "transaction_id": f"TXN_{i:08d}",
        "customer_id": customer["customer_id"],
        "account_id": customer["account_id"],
        "merchant_id": f"MERCH_{random.randint(1, 500):05d}",
        "transaction_amount": round(random.uniform(5, 2000), 2),
        "transaction_timestamp": transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "transaction_status": random.choice(transaction_statuses),
        "card_network": random.choice(card_networks),
        "merchant_category": random.choice(merchant_categories),
        "country": "US"
    })

transactions_df = pd.DataFrame(transactions)


# -----------------------------
# Fraud Signals
# -----------------------------
fraud_signals = []

for _, row in transactions_df.iterrows():
    fraud_score = round(np.random.beta(2, 8), 4)

    if fraud_score >= 0.75:
        risk_level = "High"
        rule_triggered = random.choice([
            "Velocity Check",
            "High Amount",
            "Device Mismatch",
            "Geo Anomaly"
        ])
    elif fraud_score >= 0.40:
        risk_level = "Medium"
        rule_triggered = random.choice([
            "Unusual Merchant",
            "Amount Spike",
            "New Device"
        ])
    else:
        risk_level = "Low"
        rule_triggered = "None"

    fraud_signals.append({
        "transaction_id": row["transaction_id"],
        "fraud_score": fraud_score,
        "risk_level": risk_level,
        "rule_triggered": rule_triggered,
        "device_risk_score": round(random.uniform(0, 1), 4),
        "velocity_count": random.randint(0, 12),
        "model_version": random.choice(["v1.0", "v1.1", "v1.2"]),
        "score_timestamp": row["transaction_timestamp"]
    })

fraud_signals_df = pd.DataFrame(fraud_signals)


# -----------------------------
# Disputes
# Only some transactions become disputes
# -----------------------------
disputed_transactions = transactions_df.sample(frac=0.12, random_state=42)
disputes = []

for i, (_, row) in enumerate(disputed_transactions.iterrows(), start=1):
    opened_date = pd.to_datetime(row["transaction_timestamp"]) + timedelta(days=random.randint(1, 45))

    disputes.append({
        "dispute_id": f"DISP_{i:07d}",
        "claim_id": f"CLM_{i:07d}",
        "transaction_id": row["transaction_id"],
        "dispute_reason": random.choice(dispute_reasons),
        "dispute_amount": row["transaction_amount"],
        "dispute_status": random.choice(dispute_statuses),
        "opened_date": opened_date.strftime("%Y-%m-%d"),
        "card_network": row["card_network"]
    })

disputes_df = pd.DataFrame(disputes)


# -----------------------------
# Chargeback Outcomes
# Only some disputes become chargebacks
# -----------------------------
chargeback_disputes = disputes_df.sample(frac=0.70, random_state=24)
chargebacks = []

for i, (_, row) in enumerate(chargeback_disputes.iterrows(), start=1):
    resolved_date = pd.to_datetime(row["opened_date"]) + timedelta(days=random.randint(10, 90))
    outcome = random.choice(chargeback_outcomes)

    if outcome == "Won":
        final_amount = row["dispute_amount"]
        win_loss_flag = "Win"
    elif outcome == "Lost":
        final_amount = 0
        win_loss_flag = "Loss"
    else:
        final_amount = 0
        win_loss_flag = "Pending"

    chargebacks.append({
        "chargeback_id": f"CBK_{i:07d}",
        "dispute_id": row["dispute_id"],
        "outcome": outcome,
        "win_loss_flag": win_loss_flag,
        "final_amount": final_amount,
        "resolved_date": resolved_date.strftime("%Y-%m-%d"),
        "representment_required": random.choice([True, False])
    })

chargebacks_df = pd.DataFrame(chargebacks)


# -----------------------------
# Save files locally as newline-delimited JSON
# -----------------------------
customers_df.to_json(OUTPUT_DIR / "customers.json", orient="records", lines=True)
transactions_df.to_json(OUTPUT_DIR / "transactions.json", orient="records", lines=True)
fraud_signals_df.to_json(OUTPUT_DIR / "fraud_signals.json", orient="records", lines=True)
disputes_df.to_json(OUTPUT_DIR / "disputes.json", orient="records", lines=True)
chargebacks_df.to_json(OUTPUT_DIR / "chargeback_outcomes.json", orient="records", lines=True)


print("Synthetic fintech JSON data generated successfully.")
print(f"Customers: {len(customers_df)}")
print(f"Transactions: {len(transactions_df)}")
print(f"Fraud signals: {len(fraud_signals_df)}")
print(f"Disputes: {len(disputes_df)}")
print(f"Chargebacks: {len(chargebacks_df)}")
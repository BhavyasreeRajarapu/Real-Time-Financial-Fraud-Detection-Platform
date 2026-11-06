import json
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "transactions"
N_MESSAGES = 20      # start small
DELAY_SECONDS = 0.05  # pause between messages, to mimic a stream

test = pd.read_parquet(ROOT / "dataset" / "test.parquet")

# Demo sample: 30 fraud + 170 legitimate, replayed in time order
fraud = test[test["isFraud"] == 1].sample(30, random_state=42)
legit = test[test["isFraud"] == 0].sample(170, random_state=42)
test = pd.concat([fraud, legit]).sort_values("step", kind="stable").copy()

# The saved file stores the type as is_transfer; rebuild the raw field
test["type"] = test["is_transfer"].map({1: "TRANSFER", 0: "CASH_OUT"})

columns = [
    "step", "type", "amount", "oldbalanceOrg", "oldbalanceDest",
    "orig_prev_count", "dest_prev_count", "amount_vs_orig_avg",
    "dest_prev_received", "nameOrig", "nameDest", "isFraud",
]
# to_json turns NaN into null, which is valid JSON
records = json.loads(test[columns].to_json(orient="records"))

producer = Producer({"bootstrap.servers": "localhost:9092"})

for i, rec in enumerate(records, start=1):
    producer.produce(TOPIC, key=rec["nameOrig"], value=json.dumps(rec))
    producer.poll(0)
    print(f"sent {i}/{len(records)}: step={rec['step']} type={rec['type']} amount={rec['amount']:.0f}")
    time.sleep(DELAY_SECONDS)

producer.flush()
print("done")
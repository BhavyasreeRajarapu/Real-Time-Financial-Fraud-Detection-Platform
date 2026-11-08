import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Consumer

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "alerts.db"
API_URL = "http://127.0.0.1:8000/score"

db = sqlite3.connect(DB_PATH)
db.execute("""
CREATE TABLE IF NOT EXISTS scored (
    kafka_partition INTEGER,
    kafka_offset INTEGER,
    scored_at TEXT,
    step INTEGER,
    type TEXT,
    amount REAL,
    oldbalanceOrg REAL,
    oldbalanceDest REAL,
    nameOrig TEXT,
    nameDest TEXT,
    fraud_probability REAL,
    is_fraud INTEGER,
    actual_fraud INTEGER,
    status TEXT,
    analyst_label TEXT,
    PRIMARY KEY (kafka_partition, kafka_offset)
)
""")
db.commit()

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-scorer-db-v2",   # new group name: starts from the first message
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,        # never record progress, so a failed run can't skip messages
})
consumer.subscribe(["transactions"])

total = inserted = flagged = 0
empty_polls = 0

while empty_polls < 5:                 # stop after about 5 seconds of silence
    msg = consumer.poll(1.0)
    if msg is None:
        empty_polls += 1
        continue
    if msg.error():
        print("error:", msg.error())
        continue
    empty_polls = 0

    tx = json.loads(msg.value())
    try:
        resp = requests.post(API_URL, json=tx, timeout=5)
        resp.raise_for_status()
    except requests.RequestException as e:
        print("API call failed:", e)
        break
    result = resp.json()

    total += 1
    is_fraud = bool(result["is_fraud"])
    flagged += is_fraud

    cur = db.execute(
        "INSERT OR IGNORE INTO scored VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            msg.partition(), msg.offset(),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            tx["step"], tx["type"], tx["amount"],
            tx["oldbalanceOrg"], tx["oldbalanceDest"],
            tx["nameOrig"], tx["nameDest"],
            result["fraud_probability"], int(is_fraud), tx["isFraud"],
            "new" if is_fraud else "none",   # case status
            None,                            # analyst feedback label
        ),
    )
    db.commit()
    inserted += cur.rowcount

consumer.close()
rows = db.execute("SELECT COUNT(*) FROM scored").fetchone()[0]
db.close()

print("scored this run:", total)
print("flagged this run:", flagged)
print("new rows saved:", inserted)
print("total rows in alerts.db:", rows)
import json

import requests
from confluent_kafka import Consumer

API_URL = "http://127.0.0.1:8000/score"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-scorer",
    "auto.offset.reset": "earliest",   # start from the first message
})
consumer.subscribe(["transactions"])

total = flagged = actual_fraud = caught = false_alarms = 0
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
    predicted = result["is_fraud"]
    actual = tx["isFraud"] == 1
    flagged += predicted
    actual_fraud += actual
    caught += predicted and actual
    false_alarms += predicted and not actual

    print(
        f"{total:>3} step={tx['step']} {tx['type']:<9} amount={tx['amount']:>10.0f} "
        f"-> prob={result['fraud_probability']:.4f} flagged={predicted} actual_fraud={actual}"
    )

consumer.close()
print("\nscored:", total)
print("flagged:", flagged)
print("actual fraud:", actual_fraud)
print("fraud caught:", caught)
print("false alarms:", false_alarms)
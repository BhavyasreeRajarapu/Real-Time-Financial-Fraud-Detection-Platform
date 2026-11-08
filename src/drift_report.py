import sqlite3
from pathlib import Path

import pandas as pd
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset

ROOT = Path(__file__).resolve().parent.parent
NUMERIC = ["amount", "oldbalanceOrg", "oldbalanceDest"]
CATEGORICAL = ["is_transfer"]
COLUMNS = NUMERIC + CATEGORICAL

# Reference: what the model was trained on
reference = pd.read_parquet(ROOT / "dataset" / "train.parquet")
reference = reference.sample(20000, random_state=42)[COLUMNS]

# Current: what the pipeline has scored
with sqlite3.connect(ROOT / "alerts.db") as db:
    stream = pd.read_sql("SELECT * FROM scored", db)
stream["is_transfer"] = (stream["type"] == "TRANSFER").astype(int)
current = stream[COLUMNS]

print("reference rows:", len(reference), "| current rows:", len(current))

definition = DataDefinition(
    numerical_columns=NUMERIC,
    categorical_columns=CATEGORICAL,
)
ref_ds = Dataset.from_pandas(reference, data_definition=definition)
cur_ds = Dataset.from_pandas(current, data_definition=definition)

report = Report([DataDriftPreset()])
snapshot = report.run(cur_ds, ref_ds)   # current first, then reference

out = ROOT / "drift_report.html"
snapshot.save_html(str(out))
print("saved:", out)

print("\nResults:")
for m in snapshot.dict()["metrics"]:
    print(m.get("metric_name"), "->", m.get("value"))
import json
from datetime import datetime, timezone

DRIFT_SHARE_LIMIT = 0.5     # retrain if at least half the columns drift
MIN_CURRENT_ROWS = 200      # don't trust drift on tiny samples

drift = next(
    m["value"] for m in snapshot.dict()["metrics"]
    if str(m.get("metric_name", "")).startswith("DriftedColumnsCount")
)
share = float(drift["share"])
enough_data = len(current) >= MIN_CURRENT_ROWS
retrain = enough_data and share >= DRIFT_SHARE_LIMIT

decision = {
    "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "current_rows": len(current),
    "drifted_column_share": share,
    "drift_share_limit": DRIFT_SHARE_LIMIT,
    "enough_data": enough_data,
    "retrain_recommended": retrain,
}
(ROOT / "retrain_flag.json").write_text(json.dumps(decision, indent=2))

print("\nRetraining decision:")
print(json.dumps(decision, indent=2))
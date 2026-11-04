import json
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, Field
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parent.parent

# Load the model and the exact feature order used in training
model = XGBClassifier()
model.load_model(ROOT / "models" / "xgb_baseline.json")
FEATURES = json.loads((ROOT / "models" / "features.json").read_text())

THRESHOLD = 0.5

app = FastAPI(title="Fraud Scoring API")


class Transaction(BaseModel):
    step: int
    type: Literal["TRANSFER", "CASH_OUT"]
    amount: float = Field(ge=0)
    oldbalanceOrg: float = Field(ge=0)
    oldbalanceDest: float = Field(ge=0)
    # History features: optional until we have a feature store
    orig_prev_count: int = 0
    dest_prev_count: int = 0
    amount_vs_orig_avg: Optional[float] = None
    dest_prev_received: float = 0.0


def build_features(t: Transaction) -> pd.DataFrame:
    row = {
        "step": t.step,
        "amount": t.amount,
        "oldbalanceOrg": t.oldbalanceOrg,
        "oldbalanceDest": t.oldbalanceDest,
        "is_transfer": int(t.type == "TRANSFER"),
        "amount_to_balance": t.amount / (t.oldbalanceOrg + 1),
        "drains_account": int(t.amount >= t.oldbalanceOrg),
        "orig_zero_balance": int(t.oldbalanceOrg == 0),
        "dest_zero_balance": int(t.oldbalanceDest == 0),
        "orig_prev_count": t.orig_prev_count,
        "dest_prev_count": t.dest_prev_count,
        "amount_vs_orig_avg": (
            np.nan if t.amount_vs_orig_avg is None else t.amount_vs_orig_avg
        ),
        "dest_prev_received": t.dest_prev_received,
    }
    return pd.DataFrame([row])[FEATURES]


@app.get("/health")
def health():
    return {"status": "ok", "n_features": len(FEATURES)}


@app.post("/score")
def score(t: Transaction):
    X = build_features(t)
    proba = float(model.predict_proba(X)[0, 1])
    return {
        "fraud_probability": round(proba, 4),
        "is_fraud": proba >= THRESHOLD,
        "threshold": THRESHOLD,
    }
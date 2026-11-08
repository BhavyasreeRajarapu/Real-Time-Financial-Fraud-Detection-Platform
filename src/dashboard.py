import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "alerts.db"

STATUSES = ["new", "investigating", "closed"]
LABELS = ["(none)", "confirmed fraud", "false positive"]

st.set_page_config(page_title="Fraud Analyst Dashboard", layout="wide")
st.title("Fraud Analyst Dashboard")


def load() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as db:
        return pd.read_sql("SELECT * FROM scored", db)


df = load()
if df.empty:
    st.warning("No scored transactions yet. Run src/consumer.py first.")
    st.stop()

alerts = df[df["is_fraud"] == 1].copy()

# --- Summary numbers ---
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions scored", len(df))
c2.metric("Alerts raised", len(alerts))
c3.metric("Open (new)", int((alerts["status"] == "new").sum()))
c4.metric("Confirmed fraud", int((alerts["analyst_label"] == "confirmed fraud").sum()))
c5.metric("False positives", int((alerts["analyst_label"] == "false positive").sum()))

# --- Alert queue ---
st.subheader("Alert queue")
status_filter = st.multiselect("Show case status", STATUSES, default=["new", "investigating"])
queue = alerts[alerts["status"].isin(status_filter)].sort_values(
    "fraud_probability", ascending=False
)

show_cols = [
    "kafka_offset", "step", "type", "amount", "nameOrig", "nameDest",
    "fraud_probability", "status", "analyst_label",
]
st.dataframe(queue[show_cols], use_container_width=True, hide_index=True)

# --- Case handling ---
st.subheader("Update a case")
if queue.empty:
    st.info("No alerts match this filter.")
else:
    case_id = st.selectbox("Alert (Kafka offset)", queue["kafka_offset"].tolist())
    case = alerts[alerts["kafka_offset"] == case_id].iloc[0]

    left, right = st.columns(2)
    with left:
        st.write("**Transaction details**")
        st.json({
            "type": case["type"],
            "step": int(case["step"]),
            "amount": float(case["amount"]),
            "sender": case["nameOrig"],
            "sender balance before": float(case["oldbalanceOrg"]),
            "receiver": case["nameDest"],
            "receiver balance before": float(case["oldbalanceDest"]),
            "fraud probability": float(case["fraud_probability"]),
        })
    with right:
        with st.form("case_form"):
            new_status = st.selectbox("Case status", STATUSES, index=STATUSES.index(case["status"]))
            current_label = case["analyst_label"] if case["analyst_label"] in LABELS else "(none)"
            new_label = st.selectbox("Analyst feedback label", LABELS, index=LABELS.index(current_label))
            if st.form_submit_button("Save"):
                with sqlite3.connect(DB_PATH) as db:
                    db.execute(
                        "UPDATE scored SET status = ?, analyst_label = ? "
                        "WHERE kafka_partition = ? AND kafka_offset = ?",
                        (new_status, None if new_label == "(none)" else new_label,
                         int(case["kafka_partition"]), int(case["kafka_offset"])),
                    )
                st.success("Saved")
                st.rerun()

# --- Demo-only check against the true labels ---
with st.expander("Demo check: fraud the model did not flag"):
    st.caption("Uses the simulator's true labels, which a live system would not have.")
    missed = df[(df["actual_fraud"] == 1) & (df["is_fraud"] == 0)]
    st.write(f"Missed fraud: {len(missed)}")
    if not missed.empty:
        st.dataframe(
            missed[["kafka_offset", "step", "type", "amount", "fraud_probability"]],
            use_container_width=True, hide_index=True,
        )
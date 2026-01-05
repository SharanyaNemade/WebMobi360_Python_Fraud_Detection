import os
import pandas as pd
from datetime import datetime, timedelta
import joblib
import pickle
from db import get_db_connection

model = joblib.load("model/fraud_detect_model.pkl")
preprocess = pickle.load(open("model/preprocess.pkl", "rb"))




def load_transaction_history():
    conn = get_db_connection()

    query = """
    SELECT
        TransactionId   AS TransactionID,
        senderid        AS SenderID,
        receiverid      AS ReceiverID,
        Amount,
        Timestamp,
        SenderLocation,
        ReceiverLocation
    FROM transaction_history
    """

    df = pd.read_sql(query, conn)
    conn.close()

    # ✅ FIX 1: Force numeric Amount
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

    # ✅ FIX 2: Force datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # Remove invalid rows
    df = df.dropna(subset=["Timestamp"])

    # Clean IDs
    df["SenderID"] = df["SenderID"].astype(str).str.strip()
    df["ReceiverID"] = df["ReceiverID"].astype(str).str.strip()

    return df


def predict_transaction(
        transaction_id,
        transaction_type,
        amount,
        old_org,
        new_org,
        old_dest,
        new_dest,
        sender_id,
        receiver_id,
        transaction_time,
        sender_location,
        receiver_location
):
    """
    Predict fraud risk score and fraud label from manual user input
    """

    # ---------------------------
    # Load transaction history
    # ---------------------------
    history_df = load_transaction_history()

    # Ensure Timestamp is datetime (VERY IMPORTANT for manual)
    history_df["Timestamp"] = pd.to_datetime(
        history_df["Timestamp"],
        errors="coerce"
    )

    # Drop invalid timestamps
    history_df = history_df.dropna(subset=["Timestamp"])

    # ---------------------------
    # ---------------------------
    # Convert transaction_time
    # ---------------------------
    if isinstance(transaction_time, str):
        dt = datetime.strptime(transaction_time, "%d-%m-%Y %H:%M")
    else:
        dt = transaction_time

    # ---------------------------
    # Sender-specific history
    # ---------------------------
    sender_history = history_df[
        history_df["SenderID"] == sender_id
        ]

    # ---------------------------
    # Previous sender-receiver link
    # ---------------------------
    previous_links = sender_history[
        sender_history["ReceiverID"] == receiver_id
        ]

    previous_link = 1 if not previous_links.empty else 0
    link_count = len(previous_links)

    # ---------------------------
    # Sender average transaction
    # ---------------------------
    if not sender_history.empty:
        sender_avg_amount = sender_history["Amount"].mean()
    else:
        sender_avg_amount = amount

    amount_deviation = abs(amount - sender_avg_amount)

    # ---------------------------
    # Velocity (last 24 hours)
    # ---------------------------
    """recent_txns = sender_history[
        sender_history["Timestamp"] >= (dt - timedelta(hours=24))
        ]

    transaction_velocity_24h = len(recent_txns)"""

    # ---------------------------
    # Time features
    # ---------------------------
    hour = dt.hour

    if 0 <= hour < 6:
        time_period = "Night"
    elif 6 <= hour < 12:
        time_period = "Morning"
    elif 12 <= hour < 18:
        time_period = "Afternoon"
    else:
        time_period = "Evening"

    # ---------------------------
    # Balance features
    # ---------------------------
    balance_delta_origin = abs(old_org - new_org)
    balance_delta_dest = abs(new_dest - old_dest)

    # ---------------------------
    # Transaction success
    # ---------------------------
    is_successful = int(
        abs(new_org - (old_org - amount)) < 0.05 and
        abs(new_dest - (old_dest + amount)) < 0.05
    )

    # ---------------------------
    # Mismatch & anomaly rules
    # ---------------------------
    location_mismatch = int(sender_location != receiver_location)
    origin_mismatch = int(round(new_org, 2) != round(old_org - amount, 2))
    dest_mismatch = int(round(new_dest, 2) != round(old_dest + amount, 2))

    amount_anomaly_flag = int(
        not sender_history.empty and amount > 3 * sender_avg_amount
    )

    # Ensure proper datetime
    history_df["Timestamp"] = pd.to_datetime(history_df["Timestamp"], errors="coerce")
    history_df = history_df.dropna(subset=["Timestamp"])

    # Remove duplicates
    history_df = history_df.drop_duplicates(
        subset=["TransactionID"],
        keep="first"
    )

    # Clean sender id
    history_df["SenderID"] = history_df["SenderID"].astype(str).str.strip()
    sender_id = sender_id.strip()

    sender_history = history_df[history_df["SenderID"] == sender_id]

    cutoff_time = dt - timedelta(hours=24)

    recent_txns = sender_history[
        (sender_history["Timestamp"] >= cutoff_time) &
        (sender_history["Timestamp"] <= dt) &
        (sender_history["TransactionID"] != transaction_id)
        ]

    transaction_velocity_24h = recent_txns.shape[0]

    behavior_score = 0

    if location_mismatch:
        behavior_score += 1
    if previous_link == 0:
        behavior_score += 1
    if transaction_velocity_24h > 5:
        behavior_score += 1
    if amount_deviation > sender_avg_amount:
        behavior_score += 1

    behavior_anomaly_flag = int(behavior_score >= 2)

    # ---------------------------
    # Build model input
    # ---------------------------
    data = {
        "Amount": amount,
        "senderid":sender_id,
        "receiverid":receiver_id,
        "OldBalanceOrigin": old_org,
        "NewBalanceOrigin": new_org,
        "OldBalanceDestination": old_dest,
        "NewBalanceDestination": new_dest,
        "BalanceDeltaOrigin": balance_delta_origin,
        "BalanceDeltaDestination": balance_delta_dest,
        "TransactionType": transaction_type,
        "SenderLocation": sender_location,
        "ReceiverLocation": receiver_location,
        "LocationMismatch": location_mismatch,
        "PreviousLinkWithReceiver": previous_link,
        "LinkHistoryCount": link_count,
        "SenderAvgTransactionAmount": sender_avg_amount,
        "AmountDeviationFromSenderAvg": amount_deviation,
        "TransactionVelocity24h": transaction_velocity_24h,
        "AmountAnomalyFlag": amount_anomaly_flag,
        "BehaviorAnomalyFlag": behavior_anomaly_flag,
        "IsSuccessful": is_successful,
        "Hour": hour,
        "TimePeriod": time_period
    }

    df_new = pd.DataFrame([data])

    # ---------------------------
    # Predict
    # ---------------------------
    X_new = preprocess.transform(df_new)
    if hasattr(model, "predict_proba"):
        fraud_risk_score = model.predict_proba(X_new)[0][1]
    else:
        fraud_risk_score = model.predict(X_new)[0]

    fraud_label = "Fraud" if fraud_risk_score >= 0.5 else "Not Fraud"



    # ---------------------------
    # Return result
    # ---------------------------
    fraud_reasons = []

    if amount_anomaly_flag == 1:
        fraud_reasons.append("Unusually high transaction amount compared to sender history")

    if location_mismatch == 1:
        fraud_reasons.append("Sender and receiver locations do not match")

    if transaction_velocity_24h > 5:
        fraud_reasons.append("Too many transactions in the last 24 hours")

    if previous_link == 0:
        fraud_reasons.append("No previous transaction history with this receiver")

    if origin_mismatch == 1 or dest_mismatch == 1:
        fraud_reasons.append("Balance mismatch detected after transaction")

    if not fraud_reasons:
        fraud_reasons.append("No strong fraud indicators detected")
    print(sender_history[["TransactionID", "SenderID", "Timestamp"]].sort_values("Timestamp"))

    # ---------------------------
    # Return result
    # ---------------------------
    return {
        "TransactionID": transaction_id,
        "FraudRiskScore": round(float(fraud_risk_score), 3),
        "Prediction": fraud_label,
        "Fraud_reasons":fraud_reasons,
        "IsSuccessful": is_successful,
        "TimePeriod": time_period,
        "FraudReasons": fraud_reasons,
        "TransactionType": transaction_type,
        "SenderLocation": sender_location,
        "ReceiverLocation": receiver_location,
        "LocationMismatch": location_mismatch,
        "PreviousLinkWithReceiver": previous_link,
        "LinkHistoryCount": link_count,
        "SenderAvgTransactionAmount": sender_avg_amount,
        "AmountDeviationFromSenderAvg": amount_deviation,
        "TransactionVelocity24h": transaction_velocity_24h,
        "AmountAnomalyFlag": amount_anomaly_flag,
        "BehaviorAnomalyFlag": behavior_anomaly_flag,
        "Hour": hour

    }


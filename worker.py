import json
import joblib, pickle
from kafka import KafkaConsumer
from predict_function import predict_transaction
from db_operations import upsert_prediction_history, insert_transaction_history
import requests  # to send alerts to Flask
from datetime import datetime

# Load models
model = joblib.load("model/fraud_detect_model.pkl")
preprocess = pickle.load(open("model/preprocess.pkl", "rb"))

consumer = KafkaConsumer(
    'raw_transactions',
    bootstrap_servers=['localhost:9092'],
    group_id='fraud_worker_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Worker started. Listening for transactions...")

for message in consumer:
    tx = message.value
    print(f"Processing transaction: {tx['TransactionId']}")

    # Run prediction
    result = predict_transaction(
        tx["TransactionId"], tx["TransactionType"], tx["Amount"],
        tx["oldbalanceOrg"], tx["newbalanceOrig"], tx["oldbalanceDest"], tx["newbalanceDest"],
        tx["senderid"], tx["receiverid"],
        datetime.fromisoformat(tx["Timestamp"]) if isinstance(tx["Timestamp"], str) else tx["Timestamp"],
        tx["SenderLocation"], tx["ReceiverLocation"]
    )

    # Prepare record
    record = {
        **tx,
        "FraudRiskScore": float(result["FraudRiskScore"]) * 100,
        "SourceType": "LIVE",
        "IsSuccessful": result["IsSuccessful"],
        "LocationMismatch":result["LocationMismatch"],
        "AmountAnomalyFlag": result["AmountAnomalyFlag"],
        "BehaviorAnomalyFlag": result["BehaviorAnomalyFlag"],
        "TransactionVelocity24h": result["TransactionVelocity24h"],
        "PreviousLinkWithReceiver": result["PreviousLinkWithReceiver"],
        "LinkHistoryCount": result["LinkHistoryCount"],
        "SenderAvgTransactionAmount": result["SenderAvgTransactionAmount"],
        "FraudReasons": ", ".join(result["FraudReasons"]),
        "Prediction": result["Prediction"],
        "BatchID": "live",
        "SourceFile": ""
    }



    # Save to DB
    upsert_prediction_history(record)
    insert_transaction_history(record)
    print(f"Result saved for {tx['TransactionId']}: {result['Prediction']}")

    # ALWAYS send to live stream
    live_payload = {
        "TransactionId": tx["TransactionId"],
        "Amount": tx["Amount"],
        "Prediction": result["Prediction"],
        "FraudRiskScore": float(result["FraudRiskScore"]) * 100,
        "Reason": ", ".join(result["FraudReasons"])
    }

    print("📡 Sending LIVE transaction:", live_payload)

    requests.post(
        "http://localhost:5000/send_live",
        json=live_payload,
        timeout=3
    )

    # 🚨 Send alert ONLY if Fraud
    if result["Prediction"] == "Fraud":
        alert_payload = {
            "TransactionId": tx["TransactionId"],
            "Amount": tx["Amount"],
            "FraudRiskScore": float(result["FraudRiskScore"]) * 100,
            "Reason": ", ".join(result["FraudReasons"])
        }

        print("🚨 Fraud detected – sending alert")

        requests.post(
            "http://localhost:5000/send_alert",
            json=alert_payload,
            timeout=3
        )


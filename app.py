from flask import Flask, render_template, request, jsonify
import os
import pickle, joblib, os, random, uuid
import pandas as pd
from datetime import datetime
from predict_function import predict_transaction
from db_operations import upsert_prediction_history, fetch_prediction_history, insert_transaction_history, fetch_transaction_history_grouped_by_user, fetch_transactions_for_user
from kafka import KafkaProducer
import json
from db_init import init_database
from db_loader import import_transaction_history_if_needed
from flask_socketio import SocketIO, emit


# ---------------- LOAD MODEL ----------------
model = joblib.load("model/fraud_detect_model.pkl")
preprocess = pickle.load(open("model/preprocess.pkl", "rb"))

app = Flask(__name__)


# app.config["UPLOAD_FOLDER"] = "uploads"




try:
    model = joblib.load("model/fraud_detect_model.pkl")
    preprocess = pickle.load(open("model/preprocess.pkl", "rb"))
except Exception as e:
    print("Model load failed:", e)
    model = None
    preprocess = None




# Initialize SocketIO

        # DISABLED FOR NOW TO AVOID ERRORS IF SOCKETIO IS NOT SET UP


# socketio = SocketIO(app, cors_allowed_origins="*")




socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
)



# 🔥 runs once when app starts
init_database()

# Load CSV data if needed
import_transaction_history_if_needed()




# @app.before_first_request


def startup_tasks():
    init_database()
    import_transaction_history_if_needed()



# Initialize Kafka Producer

        # Disabled for now to avoid errors if Kafka is not set up

# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
# )



producer = None

if os.getenv("ENABLE_KAFKA") == "true":
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )





# ---------------- ANALYSIS HELPER ----------------
def analyze_df(df):
    total = len(df)

    fraud_df = df[df["Prediction"] == "Fraud"]

    fraud_by_type = (
        fraud_df["TransactionType"].value_counts().to_dict()
        if "TransactionType" in fraud_df.columns
        else {}
    )


    return {
        "total": total,
        "fraud_count": len(fraud_df),
        "safe_count": total - len(fraud_df),
        "fraud_percentage": round((len(fraud_df) / total) * 100, 2) if total else 0,
        "avg_risk": round(df["FraudRiskScore"].mean(), 2),
        "max_risk": round(df["FraudRiskScore"].max(), 2),
        "fraud_by_type": fraud_by_type
    }


# ---------------- ROUTES ----------------


@app.route("/home")
def home():
    return render_template("home.html")

@app.route("/manual")
def manual_page():
    return render_template("manual_predict.html")

@app.route("/upload_csv")
def upload_page():
    return render_template("upload_csv.html")

@app.route("/live")
def live_page():
    return render_template("live_stream.html")

@app.route("/send_live", methods=["POST"])
def send_live():
    data = request.json
    print("📡 Live TX received:", data)

    socketio.emit(
        "live_tx",
        data,
        namespace="/live"
    )

    return jsonify({"status": "live_sent"})


@socketio.on("connect", namespace="/live")
def live_connect():
    print("✅ Browser connected to /live")

@socketio.on("disconnect", namespace="/live")
def live_disconnect():
    print("❌ Browser disconnected")

# ---------------- RECEIVE ALERT FROM WORKER ----------------
@app.route("/send_alert", methods=["POST"])
def send_alert():
    data = request.json
    print("➡ Flask received:", data)

    socketio.emit(
        "fraud_alert",
        data,
        namespace="/live"
    )

    return jsonify({"status": "alert_sent"})







"""@app.route("/predict_live", methods=["POST"])
def predict_live():
    # ... (Keep your existing data generation logic here: Tid, amount, etc.) ...

    transaction_data = {
        "TransactionId": Tid,
        "TransactionType": txn_type,
        "Amount": amount,
        "oldbalanceOrg": old_org,
        "newbalanceOrig": new_org,
        "oldbalanceDest": old_dest,
        "newbalanceDest": new_dest,
        "senderid": sender_id,
        "receiverid": receiver_id,
        "Timestamp": datetime.now().isoformat(),
        "SenderLocation": sender_location,
        "ReceiverLocation": receiver_location
    }

    # Push to Kafka instead of processing immediately
    producer.send('raw_transactions', value=transaction_data)

    return jsonify({"status": "Transaction queued for processing", "id": Tid})
"""

# ---------------- MANUAL PREDICTION ----------------
@app.route("/predict_manual", methods=["POST"])
def predict_manual():
    form = request.form

    result = predict_transaction(
        form["Tid"],
        form["type"],
        float(form["amount"]),
        float(form["oldbalanceOrg"]),
        float(form["newbalanceOrig"]),
        float(form["oldbalanceDest"]),
        float(form["newbalanceDest"]),
        form["SenderID"],
        form["ReceiverID"],
        datetime.strptime(form["transaction_time"], "%Y-%m-%dT%H:%M"),
        form["SenderLocation"],
        form["ReceiverLocation"]
    )

    FraudRiskScore = float(result["FraudRiskScore"]) * 100

    upsert_prediction_history({
        "TransactionId": result["TransactionID"],
        "Timestamp": datetime.now(),
        "TransactionType": form["type"],
        "Amount": float(form["amount"]),
        "senderid": form["SenderID"],
        "receiverid": form["ReceiverID"],
        "oldbalanceOrg": float(form["oldbalanceOrg"]),
        "newbalanceOrig": float(form["newbalanceOrig"]),
        "oldbalanceDest": float(form["oldbalanceDest"]),
        "newbalanceDest": float(form["newbalanceDest"]),
        "IsSuccessful": result["IsSuccessful"],
        "SenderLocation": form["SenderLocation"],
        "ReceiverLocation": form["ReceiverLocation"],
        "AmountAnomalyFlag": result["AmountAnomalyFlag"],
        "BehaviorAnomalyFlag": result["BehaviorAnomalyFlag"],
        "TransactionVelocity24h": result["TransactionVelocity24h"],
        "PreviousLinkWithReceiver": result["PreviousLinkWithReceiver"],
        "LinkHistoryCount": result["LinkHistoryCount"],
        "SenderAvgTransactionAmount": result["SenderAvgTransactionAmount"],
        "FraudReasons": ", ".join(result["FraudReasons"]),
        "Prediction": result["Prediction"],
        "FraudRiskScore": FraudRiskScore,
        "SourceType": "MANUAL",
        "BatchID": "MANUAL",
        "SourceFile": ""
    })

    insert_transaction_history({
        "TransactionId": result["TransactionID"],
        "Timestamp": datetime.now(),
        "TransactionType": form["type"],
        "Amount": float(form["amount"]),
        "senderid": form["SenderID"],
        "receiverid": form["ReceiverID"],
        "oldbalanceOrg": float(form["oldbalanceOrg"]),
        "newbalanceOrig": float(form["newbalanceOrig"]),
        "oldbalanceDest": float(form["oldbalanceDest"]),
        "newbalanceDest": float(form["newbalanceDest"]),
        "IsSuccessful": result["IsSuccessful"],
        "SenderLocation": form["SenderLocation"],
        "ReceiverLocation": form["ReceiverLocation"],
        "AmountAnomalyFlag": result["AmountAnomalyFlag"],
        "BehaviorAnomalyFlag": result["BehaviorAnomalyFlag"],
        "TransactionVelocity24h": result["TransactionVelocity24h"],
        "PreviousLinkWithReceiver": result["PreviousLinkWithReceiver"],
        "LinkHistoryCount": result["LinkHistoryCount"],
        "SenderAvgTransactionAmount": result["SenderAvgTransactionAmount"],
        "LocationMismatch": result["LocationMismatch"],
        "Prediction": result["Prediction"],
        "FraudRiskScore": FraudRiskScore,
        "SourceType": "MANUAL",
        "BatchID": "MANUAL",
        "SourceFile": ""
    })



    risk_class = "low" if FraudRiskScore < 30 else "medium" if FraudRiskScore < 70 else "high"

    return render_template(
        "predict.html",
        FraudRiskScore=f"{FraudRiskScore:.2f}",
        prediction=result["Prediction"],
        risk_class=risk_class,
        fraud_reasons=result["FraudReasons"]
    )

# ---------------- HISTORY ----------------
@app.route("/history")
def history():
    filter_type = request.args.get("type", "ALL")
    records = fetch_prediction_history(filter_type)
    return render_template("history.html", records=records, selected=filter_type)

# ---------------- ANALYSIS ----------------
@app.route("/analysis")
def analysis():
    records = fetch_prediction_history("ALL")
    total_transactions = len(records)

    if not records:
        return render_template("analysis.html", data_available=False)

    df = pd.DataFrame(records)
    analysis_data = analyze_df(df)

    recent = df.sort_values("Timestamp").tail(10)

    return render_template(
        "analysis.html",
        data_available=True,
        total=analysis_data["total"],
        fraud_count=analysis_data["fraud_count"],
        safe_count=analysis_data["safe_count"],
        fraud_by_type=analysis_data["fraud_by_type"],
        prob_trend=recent["FraudRiskScore"].tolist(),
        timestamps=recent["Timestamp"].astype(str).tolist()
    )

@app.route('/user-analysis')
def user_analysis():
    data = fetch_transaction_history_grouped_by_user()
    return render_template('user_analysis.html', data=data)

@app.route('/user-analysis/<user_id>')
def user_detail(user_id):
    history = fetch_transactions_for_user(user_id)
    return render_template(
        'user_detail.html',
        history=history,
        user_id=user_id
    )




@app.route("/predict_csv", methods=["POST"])
def predict_csv():

    file = request.files.get("csvfile")

    if not file or file.filename == "":
        return "No file uploaded", 400

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    filepath = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
    file.save(filepath)

    df = pd.read_csv(filepath)

    # 🔥 Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.replace("\ufeff", "")
        .str.lower()
    )

    batch_id = f"CSV_{uuid.uuid4().hex[:8]}"
    source_file = file.filename

    prediction_rows = []   # ✅ store rows for UI + analysis

    for _, row in df.iterrows():

            transaction_time = pd.to_datetime(row["timestamp"], format="%d-%m-%Y %H:%M", errors="coerce")

            result = predict_transaction(
                transaction_id=row["transactionid"],
                transaction_type=row["transactiontype"],
                amount=float(row["amount"]),
                old_org=float(row["oldbalanceorigin"]),
                new_org=float(row["newbalanceorigin"]),
                old_dest=float(row["oldbalancedestination"]),
                new_dest=float(row["newbalancedestination"]),
                sender_id=row["senderid"],
                receiver_id=row["receiverid"],
                transaction_time=transaction_time,
                sender_location=row["senderlocation"],
                receiver_location=row["receiverlocation"]
            )

            FraudRiskScore = float(result["FraudRiskScore"]) * 100

            record = {
                "TransactionId": row["transactionid"],
                "Timestamp": transaction_time,
                "TransactionType": row["transactiontype"],
                "Amount": float(row["amount"]),
                "senderid": row["senderid"],
                "receiverid": row["receiverid"],
                "oldbalanceOrg": float(row["oldbalanceorigin"]),
                "newbalanceOrig": float(row["newbalanceorigin"]),
                "oldbalanceDest": float(row["oldbalancedestination"]),
                "newbalanceDest": float(row["newbalancedestination"]),
                "IsSuccessful": result["IsSuccessful"],
                "SenderLocation": row["senderlocation"],
                "ReceiverLocation": row["receiverlocation"],
                "AmountAnomalyFlag": result["AmountAnomalyFlag"],
                "BehaviorAnomalyFlag": result["BehaviorAnomalyFlag"],
                "TransactionVelocity24h": result["TransactionVelocity24h"],
                "PreviousLinkWithReceiver": result["PreviousLinkWithReceiver"],
                "LinkHistoryCount": result["LinkHistoryCount"],
                "SenderAvgTransactionAmount": result["SenderAvgTransactionAmount"],
                "FraudReasons": ", ".join(result["FraudReasons"]),
                "Prediction": result["Prediction"],
                "FraudRiskScore": FraudRiskScore,
                "SourceType": "CSV",
                "BatchID": batch_id,
                "SourceFile": source_file
            }

            # ✅ Store in DB
            upsert_prediction_history(record)
            insert_transaction_history(record)

            # ✅ Store for UI
            prediction_rows.append(record)




    # ---------------- ANALYSIS ----------------
    result_df = pd.DataFrame(prediction_rows)

    analysis = analyze_df(result_df)

    chart_data = {
        "fraud_vs_safe": [
            analysis["fraud_count"],
            analysis["safe_count"]
        ],
        "fraud_by_type": analysis["fraud_by_type"],
        "risk_scores": result_df["FraudRiskScore"]
        .fillna(0)
        .astype(float)
        .tolist()
    }

    return render_template(
        "csv_results.html",
        records=result_df.to_dict(orient="records"),
        analysis=analysis,
        chart_data=chart_data,
        batch_id=batch_id,
        source_file=source_file
    )



# if __name__ == "__main__":
#     socketio.run(app, debug=True, allow_unsafe_werkzeug=True)



        # REPLACED


import os
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=8000)


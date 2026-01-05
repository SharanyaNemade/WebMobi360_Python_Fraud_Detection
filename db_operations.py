from db import get_db_connection


def upsert_prediction_history(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO prediction_history (
        TransactionId, Timestamp, TransactionType, Amount,
        senderid, receiverid,
        oldbalanceOrg, newbalanceOrig,
        oldbalanceDest, newbalanceDest,
        IsSuccessful,
        SenderLocation, ReceiverLocation,
        AmountAnomalyFlag, BehaviorAnomalyFlag, TransactionVelocity24h,
        PreviousLinkWithReceiver,LinkHistoryCount,SenderAvgTransactionAmount,
        FraudReasons, Prediction, FraudRiskScore,
        SourceType, BatchID, SourceFile
    )
    VALUES (
        %s,%s,%s,%s,
        %s,%s,
        %s,%s,
        %s,%s,
        %s,
        %s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        Timestamp = VALUES(Timestamp),
        Amount = VALUES(Amount),
        Prediction = VALUES(Prediction),
        FraudRiskScore = VALUES(FraudRiskScore),
        FraudReasons = VALUES(FraudReasons),
        AmountAnomalyFlag = VALUES(AmountAnomalyFlag),
        BehaviorAnomalyFlag = VALUES(BehaviorAnomalyFlag),
        TransactionVelocity24h = VALUES(TransactionVelocity24h),
        SourceType = VALUES(SourceType),
        BatchID = VALUES(BatchID),
        SourceFile = VALUES(SourceFile)
    """

    values = (
        data["TransactionId"],
        data["Timestamp"],
        data["TransactionType"],
        data["Amount"],
        data["senderid"],
        data["receiverid"],
        data["oldbalanceOrg"],
        data["newbalanceOrig"],
        data["oldbalanceDest"],
        data["newbalanceDest"],
        data["IsSuccessful"],
        data["SenderLocation"],
        data["ReceiverLocation"],
        data["AmountAnomalyFlag"],
        data["BehaviorAnomalyFlag"],
        data["TransactionVelocity24h"],
        data["PreviousLinkWithReceiver"],
        data["LinkHistoryCount"],
        data["SenderAvgTransactionAmount"],
        data["FraudReasons"],
        data["Prediction"],
        data["FraudRiskScore"],
        data["SourceType"],
        data["BatchID"],
        data["SourceFile"]
    )

    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()


def fetch_prediction_history(source_type=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if source_type and source_type != "ALL":
        cursor.execute(
            """
            SELECT *
            FROM prediction_history
            WHERE SourceType = %s
            ORDER BY Timestamp DESC
            """,
            (source_type,)
        )
    else:
        cursor.execute(
            """
            SELECT *
            FROM prediction_history
            ORDER BY Timestamp DESC
            """
        )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows



def insert_transaction_history(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO transaction_history (
        TransactionID, Timestamp, TransactionType, Amount,
        SenderID, ReceiverID,
        OldBalanceOrigin, NewBalanceOrigin,
        OldBalanceDestination, NewBalanceDestination,
        SenderLocation, ReceiverLocation,
        AmountAnomalyFlag, BehaviorAnomalyFlag, TransactionVelocity24h,
        PreviousLinkWithReceiver, LinkHistoryCount, SenderAvgTransactionAmount,
        IsFraud, FraudRiskScore,
        SourceType, BatchID, SourceFile
    )
    VALUES (
        %s,%s,%s,%s,
        %s,%s,
        %s,%s,
        %s,%s,
        %s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,
        %s,%s,%s
    )
    """

    values = (
        data["TransactionId"],
        data["Timestamp"],
        data["TransactionType"],
        data["Amount"],
        data["senderid"],
        data["receiverid"],
        data["oldbalanceOrg"],
        data["newbalanceOrig"],
        data["oldbalanceDest"],
        data["newbalanceDest"],
        data["SenderLocation"],
        data["ReceiverLocation"],
        data["AmountAnomalyFlag"],
        data["BehaviorAnomalyFlag"],
        data["TransactionVelocity24h"],
        data["PreviousLinkWithReceiver"],
        data["LinkHistoryCount"],
        data["SenderAvgTransactionAmount"],
        data["Prediction"],
        data["FraudRiskScore"],
        data["SourceType"],
        data["BatchID"],
        data["SourceFile"]
    )

    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()

def fetch_transaction_history_grouped_by_user():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT
        SenderID AS user_id,
        COUNT(*) AS total_transactions,

        SUM(CASE WHEN IsFraud = '1' THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN IsFraud = '0' THEN 1 ELSE 0 END) AS safe_count,

        ROUND(AVG(CAST(FraudRiskScore AS DECIMAL(5,2))), 2) AS avg_risk_score,
        ROUND(SUM(CAST(Amount AS DECIMAL(10,2))), 2) AS total_amount,

        COUNT(DISTINCT ReceiverID) AS unique_receivers
    FROM transaction_history
    GROUP BY SenderID
    ORDER BY fraud_count DESC, total_transactions DESC
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows


def fetch_transactions_for_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT
        TransactionID,
        Timestamp,
        Amount,
        TransactionType,
        ReceiverID,
        FraudRiskScore,
        IsFraud
    FROM transaction_history
    WHERE SenderID = %s
    ORDER BY Timestamp DESC
    """

    cursor.execute(query, (user_id,))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

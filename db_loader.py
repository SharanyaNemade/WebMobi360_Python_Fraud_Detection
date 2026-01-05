import os
import csv
from db import get_db_connection

CSV_FOLDER = "data"
CSV_FILE = "transaction_history.csv"


def table_exists(cursor):
    cursor.execute("""
        SHOW TABLES LIKE 'transaction_history'
    """)
    return cursor.fetchone() is not None


def table_has_data(cursor):
    cursor.execute("""
        SELECT COUNT(*) FROM transaction_history
    """)
    return cursor.fetchone()[0] > 0


def import_transaction_history_if_needed():
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1️⃣ Check table exists
    if not table_exists(cursor):
        print("❌ transaction_history table does NOT exist. Skipping CSV import.")
        return

    # 2️⃣ Check if data already exists
    if table_has_data(cursor):
        print("ℹ️ transaction_history already has data. Skipping CSV import.")
        return

    csv_path = os.path.join(CSV_FOLDER, CSV_FILE)

    if not os.path.exists(csv_path):
        print("❌ CSV file not found:", csv_path)
        return

    print("📥 Importing transaction_history CSV...")

    with open(csv_path, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)

        insert_query = """
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
            %(TransactionID)s, %(Timestamp)s, %(TransactionType)s, %(Amount)s,
            %(SenderID)s, %(ReceiverID)s,
            %(OldBalanceOrigin)s, %(NewBalanceOrigin)s,
            %(OldBalanceDestination)s, %(NewBalanceDestination)s,
            %(SenderLocation)s, %(ReceiverLocation)s,
            %(AmountAnomalyFlag)s, %(BehaviorAnomalyFlag)s, %(TransactionVelocity24h)s,
            %(PreviousLinkWithReceiver)s, %(LinkHistoryCount)s, %(SenderAvgTransactionAmount)s,
            %(IsFraud)s, %(FraudRiskScore)s,
            %(SourceType)s, %(BatchID)s, %(SourceFile)s
        )
        """

        for row in reader:
            cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ CSV import completed successfully.")

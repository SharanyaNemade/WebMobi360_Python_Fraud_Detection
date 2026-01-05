import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

transaction_types = ["TRANSFER", "PAYMENT", "CASH_OUT"]
locations = ["Banglore", "Hyderabad", "chennai", "Delhi","Mumbai"]

print("Starting to generate random transactions...")

try:
    while True:
        txn_id = f"TXN{random.randint(1,1000)}"
        txn_type = random.choice(transaction_types)
        amount = round(random.uniform(100, 10000), 2)
        oldbalanceOrg = round(random.uniform(amount, amount+10000), 2)
        newbalanceOrig = oldbalanceOrg - amount
        oldbalanceDest = round(random.uniform(0, 5000), 2)
        newbalanceDest = oldbalanceDest + amount
        sender_id = f"CUST_SEND{random.randint(1,100)}"
        receiver_id = f"CUST_RECV{random.randint(100,200)}"
        sender_location = random.choice(locations)
        receiver_location = random.choice(locations)
        timestamp = datetime.now().isoformat()

        transaction = {
            "TransactionId": txn_id,
            "TransactionType": txn_type,
            "Amount": amount,
            "oldbalanceOrg": oldbalanceOrg,
            "newbalanceOrig": newbalanceOrig,
            "oldbalanceDest": oldbalanceDest,
            "newbalanceDest": newbalanceDest,
            "senderid": sender_id,
            "receiverid": receiver_id,
            "Timestamp": timestamp,
            "SenderLocation": sender_location,
            "ReceiverLocation": receiver_location
        }

        producer.send('raw_transactions', value=transaction)
        print(f"Sent transaction: {txn_id}")
        time.sleep(3)  # Send every 3 seconds

except KeyboardInterrupt:
    print("Stopping transaction generator...")

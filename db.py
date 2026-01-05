import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",          # XAMPP default
        database="fraud_detection_database",
        port=3306
    )

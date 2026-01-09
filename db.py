# FOR SYSTEM TESTING ONLY




import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",          # XAMPP default
        database="fraud_detection_database",
        port=3306
    )





        # FOR DEPLOYMENT ONLY


# import os
# import mysql.connector

# def get_db_connection():
#     return mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         port=int(os.getenv("DB_PORT", 3306)),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME"),
#         ssl_disabled=False
#     )











#   OPTIONAL


# import mysql.connector
# import os

# def get_db_connection():
#     return mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME"),
#         port=int(os.getenv("DB_PORT", 3306))
#     )

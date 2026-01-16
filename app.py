import mysql.connector
from dotenv import load_dotenv
import os
import csv

load_dotenv()

# Get credentials
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")

# Connect to MySQL
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

with open("follows_sample.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES (%s, %s)",
            (row["USER_ID"], row["FOLLOWS_ID"])
        )

conn.commit()
cursor.close()
conn.close()
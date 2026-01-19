import mysql.connector
from dotenv import load_dotenv
import os
import csv
import time

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

cursor.execute("TRUNCATE TABLE follows")
conn.commit()

with open("follows.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES (%s, %s)",
            (row["USER_ID"], row["FOLLOWS_ID"])
        )

start_time = time.time()
tweet_count = 0
with open("tweet.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute(
            "INSERT INTO tweet (user_id, tweet_text) VALUES (%s, %s)",
            (row["USER_ID"], row["TWEET_TEXT"])
        )
        conn.commit()
        tweet_count += 1
end_time = time.time()

elapsed = end_time - start_time
tps = tweet_count / elapsed

print(f"Inserted {tweet_count} tweets in {elapsed:.2f} seconds")
print(f"Throughput: {tps:.2f} tweets/sec")

cursor.close()
conn.close()
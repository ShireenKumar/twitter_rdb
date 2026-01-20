import mysql.connector
from dotenv import load_dotenv
import os
import csv
import time
import random
from twitter_objects import Tweet


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

# cursor.execute("TRUNCATE TABLE follows")
# conn.commit()

# load follows
follow_count = 0
with open("follows.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cursor.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES (%s, %s)",
            (row["USER_ID"], row["FOLLOWS_ID"])
        )
        follow_count += 1

conn.commit()

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

# Grab the max and min
cursor.execute("SELECT MIN(followee_id), MAX(followee_id) FROM FOLLOWS")
min_user, max_user = cursor.fetchone()

iterations = 1000 
start_time = time.time()


for _ in range(iterations):
    user_id = random.randint(min_user, max_user)

    # select top 10 posts from users followers 
    # Join follows and tweets on followee_id and user_id
    # Grab the followers with the user_id and order by timestamp
    cursor.execute(
        """
        SELECT t.user_id, t.tweet_text, t.tweet_ts
        FROM tweet t JOIN follows f ON t.user_id = f.followee_id
        WHERE f.follower_id = %s
        ORDER BY t.tweet_ts DESC
        LIMIT 10
    """,
        (user_id,)
    )

    rows = cursor.fetchall() # list of tuples

    timeline = []
    for row in rows:
        tweet = Tweet(
            tweet_id = row[0],
            user_id = row[1],
            tweet_text = row[2],
            tweet_ts = row[3]
        )
        timeline.append(tweet)
end_time = time.time()
cursor.close()
conn.close()
# do we add print statements here to calculate timeline results?

# cursor.execute("TRUNCATE TABLE follows")
# conn.commit()
from dotenv import load_dotenv
import os
import csv
import time
import random
from twitter_api import TwitterAPI
from twitter_objects import Tweet

load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")

api = TwitterAPI(user=user, password=password, database=database, host=host)

# load follows.csv
follow_count = 0
with open("follows.csv", "r") as f:
    reader = csv.DictReader(f)
    cursor = api.con.cursor()

    for row in reader:
        cursor.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES (%s, %s)",
            (row["USER_ID"], row["FOLLOWS_ID"])
        )
        follow_count += 1

    api.con.commit()
    cursor.close()

# posting tweets performance test
start_time = time.time()
tweet_count = 0

with open("tweet.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        t = Tweet(
            tweet_id=None,
            user_id=int(row["USER_ID"]),
            tweet_text=row["TWEET_TEXT"],
            tweet_ts=None
        )
        api.postTweet(t)
        tweet_count += 1

end_time = time.time()
elapsed = end_time - start_time
tps = tweet_count / elapsed

print(f"Inserted {tweet_count} tweets in {elapsed:.2f} seconds")
print(f"Throughput: {tps:.2f} tweets/sec")

# timeline performance test

cursor = api.con.cursor()
cursor.execute("SELECT MIN(followee_id), MAX(followee_id) FROM follows")
min_user, max_user = cursor.fetchone()
cursor.close()

iterations = 1000
start_time = time.time()

for _ in range(iterations):
    user_id = random.randint(min_user, max_user)
    timeline = api.getTimeline(user_id)

end_time = time.time()
elapsed = end_time - start_time
tps = iterations / elapsed

print(f"Timeline fetches: {iterations} in {elapsed:.2f} seconds")
print(f"Throughput: {tps:.2f} timelines/sec")

api.close()

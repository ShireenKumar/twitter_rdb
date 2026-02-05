'''
Contributions: Shireen created this file.
Both Shireen and Arshia worked on this file together.
Modified to use Redis instead of MySQL.
'''
import csv
import time
import random
from twitter_api_redis import TwitterAPI
from twitter_objects import Tweet

# Initialize Redis API
api = TwitterAPI(host="localhost", port=6379, db=0)

# Clear existing data
api.clearTweets()
api.clearFollows()

# Load follows.csv
follow_count = 0
follows_start = time.time()

with open("follows.csv", "r") as f:
    reader = csv.DictReader(f)
    
    batch_size = 1000
    batch = []
    
    for row in reader:
        batch.append((int(row["USER_ID"]), int(row["FOLLOWS_ID"])))
        follow_count += 1
        
        if len(batch) >= batch_size:
            for follower_id, followee_id in batch:
                api.addFollow(follower_id, followee_id)
            batch = []
    
    if batch:
        for follower_id, followee_id in batch:
            api.addFollow(follower_id, followee_id)

# Posting tweets performance test
print("\npostTweet")
post_start = time.time()
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

post_end = time.time()
post_elapsed = post_end - post_start
post_tps = tweet_count / post_elapsed

print(f"Inserted {tweet_count} tweets in {post_elapsed:.2f} seconds")
print(f"postTweet : {post_tps:.2f} tweets/sec")

# Timeline performance test
print("\ngetTimeline")

all_following_keys = api.redis_client.keys("user:*:following")
user_ids = [int(key.split(":")[1]) for key in all_following_keys]

if not user_ids:
    api.close()
    exit(1)

min_user = min(user_ids)
max_user = max(user_ids)

iterations = 1000
timeline_start = time.time()

for _ in range(iterations):
    user_id = random.randint(min_user, max_user)
    timeline = api.getTimeline(user_id)

timeline_end = time.time()
timeline_elapsed = timeline_end - timeline_start
timeline_tps = iterations / timeline_elapsed

print(f"Timeline fetches: {iterations} in {timeline_elapsed:.2f} seconds")
print(f"getTimeline: {timeline_tps:.2f} timelines/sec")

api.close()

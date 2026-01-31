'''
Contributions: Both Shireen and Arshia worked on this file.
Modified to use Redis instead of MySQL.

Storage Model:
1. tweet:{tweet_id} -> Hash with fields: user_id, tweet_text, tweet_ts
2. user:{user_id}:tweets -> Sorted Set (score=timestamp, member=tweet_id)
3. user:{user_id}:timeline -> Sorted Set (score=timestamp, member=tweet_id)
4. user:{user_id}:following -> Set of user_ids
5. user:{user_id}:followers -> Set of user_ids
6. tweet_counter -> Integer counter for generating tweet IDs
'''

import redis
import time
from typing import List
from twitter_objects import Tweet


class TwitterAPI:
    """
    API for Twitter-like operations on Redis database.
    Uses fan-out on write strategy for timelines.
    """

    def __init__(self, host="localhost", port=6379, db=0):
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            decode_responses=True
        )

    def close(self):
        """Close the Redis connection"""
        self.redis_client.close()

    def clearFollows(self):
        """Clear all follow relationships"""
        # Get all following keys
        keys = self.redis_client.keys("user:*:following")
        # Appending the list of following and followers to delete 
        keys.extend(self.redis_client.keys("user:*:followers"))
        if keys:
            # Deleted all keys
            self.redis_client.delete(*keys)

    def clearTweets(self):
        """Clear all tweets and timelines"""
        # Get all tweet-related keys
        keys = self.redis_client.keys("tweet:*")
        # Appending all of the user tweet list
        keys.extend(self.redis_client.keys("user:*:tweets"))
        keys.extend(self.redis_client.keys("user:*:timeline"))
        if keys:
            self.redis_client.delete(*keys)
        # Reset tweet counter
        self.redis_client.delete("tweet_counter")

    def postTweet(self, tweet: Tweet):
        """
        Insert a tweet into Redis and fan out to all followers' timelines.
        """
        # Generate unique tweet ID
        tweet_id = self.redis_client.incr("tweet_counter")
        timestamp = time.time()
        
        # Creating the key name which is the primary key 'tweet_id'
        tweet_key = f"tweet:{tweet_id}"
        # Storing the tweet_key info in a hashset with everything listed in tweet object
        self.redis_client.hset(tweet_key, mapping={
            "user_id": tweet.user_id,
            "tweet_text": tweet.tweet_text,
            "tweet_ts": timestamp
        })
        
        # Add to user's own tweets (sorted set by timestamp)
        # A sorted set ordered by the timestamp
        self.redis_client.zadd(
            f"user:{tweet.user_id}:tweets",
            {tweet_id: timestamp}
        )
        
        # Add new tweet to all followers timelines
        followers = self.redis_client.smembers(f"user:{tweet.user_id}:followers")
        
        if followers:
            # Going through each follower
            for follower_id in followers:
                # Creating a sorted set for the follower id using the timestamp
                self.redis_client.zadd(f"user:{follower_id}:timeline", {tweet_id: timestamp})


    def getTimeline(self, user_id: int, limit: int = 10) -> List[Tweet]:
        """
        Get the most recent tweets from users followed by user_id.
        Since we use fan-out on write, this is a simple sorted set range query.
        """
        # Get latest tweet IDs from user's timeline (sorted set, highest scores first)
        # zrevrange --> gets a range from sorted set in reverse order, get the newer tweet id first 
        tweet_ids = self.redis_client.zrevrange(f"user:{user_id}:timeline", 0, limit - 1)
        
        # if there are no tweet ids
        if not tweet_ids:
            return []
        
        # Convert to Tweet objects
        timeline = []
        for tweet_id in tweet_ids:
            tweet_data = self.redis_client.hgetall(f"{tweet_id}")
            if tweet_data:
                tweet = Tweet(
                    tweet_id=int(tweet_id),
                    user_id=int(tweet_data["user_id"]),
                    tweet_text=tweet_data["tweet_text"],
                    tweet_ts=float(tweet_data["tweet_ts"])
                )
                timeline.append(tweet)
        return timeline
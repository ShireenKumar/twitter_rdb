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
        keys.extend(self.redis_client.keys("user:*:followers"))
        if keys:
            self.redis_client.delete(*keys)

    def clearTweets(self):
        """Clear all tweets and timelines"""
        # Get all tweet-related keys
        keys = self.redis_client.keys("tweet:*")
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
        
        # Store tweet data as hash
        tweet_key = f"tweet:{tweet_id}"
        self.redis_client.hset(tweet_key, mapping={
            "user_id": tweet.user_id,
            "tweet_text": tweet.tweet_text,
            "tweet_ts": timestamp
        })
        
        # Add to user's own tweets (sorted set by timestamp)
        self.redis_client.zadd(
            f"user:{tweet.user_id}:tweets",
            {tweet_id: timestamp}
        )
        
        # Fan out: Add to all followers' timelines
        followers = self.redis_client.smembers(f"user:{tweet.user_id}:followers")
        
        if followers:
            # Use pipeline for efficient batch operations
            pipeline = self.redis_client.pipeline()
            for follower_id in followers:
                pipeline.zadd(
                    f"user:{follower_id}:timeline",
                    {tweet_id: timestamp}
                )
            pipeline.execute()

    def getTimeline(self, user_id: int, limit: int = 10) -> List[Tweet]:
        """
        Get the most recent tweets from users followed by user_id.
        Since we use fan-out on write, this is a simple sorted set range query.
        """
        # Get latest tweet IDs from user's timeline (sorted set, highest scores first)
        tweet_ids = self.redis_client.zrevrange(
            f"user:{user_id}:timeline",
            0,
            limit - 1
        )
        
        if not tweet_ids:
            return []
        
        # Fetch full tweet data using pipeline
        pipeline = self.redis_client.pipeline()
        for tweet_id in tweet_ids:
            pipeline.hgetall(f"tweet:{tweet_id}")
        
        tweets_data = pipeline.execute()
        
        # Convert to Tweet objects
        timeline = []
        for tweet_id, tweet_data in zip(tweet_ids, tweets_data):
            if tweet_data:
                tweet = Tweet(
                    tweet_id=int(tweet_id),
                    user_id=int(tweet_data["user_id"]),
                    tweet_text=tweet_data["tweet_text"],
                    tweet_ts=float(tweet_data["tweet_ts"])
                )
                timeline.append(tweet)
        
        return timeline

    def getFollowers(self, user_id: int) -> List[int]:
        """Get list of user IDs who follow this user"""
        followers = self.redis_client.smembers(f"user:{user_id}:followers")
        return [int(f) for f in followers]

    def getFollowees(self, user_id: int) -> List[int]:
        """Get list of user IDs that this user follows"""
        followees = self.redis_client.smembers(f"user:{user_id}:following")
        return [int(f) for f in followees]

    def getTweets(self, user_id: int) -> List[Tweet]:
        """Get all tweets by a specific user"""
        # Get tweet IDs from user's tweets sorted set
        tweet_ids = self.redis_client.zrevrange(
            f"user:{user_id}:tweets",
            0,
            -1  # Get all tweets
        )
        
        if not tweet_ids:
            return []
        
        # Fetch full tweet data
        pipeline = self.redis_client.pipeline()
        for tweet_id in tweet_ids:
            pipeline.hgetall(f"tweet:{tweet_id}")
        
        tweets_data = pipeline.execute()
        
        # Convert to Tweet objects
        tweets = []
        for tweet_id, tweet_data in zip(tweet_ids, tweets_data):
            if tweet_data:
                tweet = Tweet(
                    tweet_id=int(tweet_id),
                    user_id=int(tweet_data["user_id"]),
                    tweet_text=tweet_data["tweet_text"],
                    tweet_ts=float(tweet_data["tweet_ts"])
                )
                tweets.append(tweet)
        
        return tweets

    def addFollow(self, follower_id: int, followee_id: int):
        """
        Add a follow relationship.
        This method is needed for loading the follows.csv data.
        """
        # Add to follower's following set
        self.redis_client.sadd(f"user:{follower_id}:following", followee_id)
        # Add to followee's followers set
        self.redis_client.sadd(f"user:{followee_id}:followers", follower_id)
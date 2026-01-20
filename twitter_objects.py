from typing import Optional
from datetime import datetime


class Tweet:
    '''represents a tweet object from the tweet database table'''
    def __init__(self, tweet_id: Optional[int], user_id: int, 
                 tweet_text: str, tweet_ts: Optional[datetime] = None):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_text = tweet_text
        self.tweet_ts = tweet_ts

    
    def __repr__(self):
        return f"Tweet(id={self.tweet_id}, user={self.user_id}, text='{self.tweet_text[:30]}...')"
import mysql.connector
from typing import List
from twitter_objects import Tweet

class TwitterAPI:
    """
    API for Twitter-like operations on MySQL database.
    """

    def __init__(self, user, password, database, host="localhost"):
        self.con = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def close(self):
        """ Close or release a connection back to the connection pool """
        self.con.close()
        self.con = None

    def clearFollows(self):
        cursor = self.con.cursor()
        cursor.execute("TRUNCATE TABLE follows")
        self.con.commit()
        cursor.close()
    
    def clearTweets(self):
        cursor = self.con.cursor()
        cursor.execute("TRUNCATE TABLE tweet")
        self.con.commit()
        cursor.close()

    def postTweet(self, tweet: Tweet):
        """
        Insert a tweet into the database.
        """
        cursor = self.con.cursor()
        sql = """
            INSERT INTO tweet (user_id, tweet_text)
            VALUES (%s, %s)
        """
        cursor.execute(sql, (tweet.user_id, tweet.tweet_text))
        self.con.commit()
        cursor.close()

    def getTimeline(self, user_id: int, limit: int = 10) -> List[Tweet]:
        """
        return the most recent tweets from users followed by user_id
        """
        cursor = self.con.cursor()
        sql = """
            SELECT t.tweet_id, t.user_id, t.tweet_text, t.tweet_ts
            FROM tweet t
            JOIN follows f ON t.user_id = f.followee_id
            WHERE f.follower_id = %s
            ORDER BY t.tweet_ts DESC
            LIMIT %s
        """
        cursor.execute(sql, (user_id, limit))
        rows = cursor.fetchall()
        cursor.close()

        timeline = []
        for row in rows:
            tweet = Tweet(
                tweet_id=row[0],
                user_id=row[1],
                tweet_text=row[2],
                tweet_ts=row[3]
            )
            timeline.append(tweet)

        return timeline

    # extra api calls - ask ta if we need these
    def getFollowers(self, user_id: int) -> List[int]:
        cursor = self.con.cursor()
        cursor.execute(
            "SELECT follower_id FROM follows WHERE followee_id = %s",
            (user_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        return [r[0] for r in rows]

    def getFollowees(self, user_id: int) -> List[int]:
        cursor = self.con.cursor()
        cursor.execute(
            "SELECT followee_id FROM follows WHERE follower_id = %s",
            (user_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        return [r[0] for r in rows]

    def getTweets(self, user_id: int) -> List[Tweet]:
        cursor = self.con.cursor()
        cursor.execute(
            """
            SELECT tweet_id, user_id, tweet_text, tweet_ts
            FROM tweet
            WHERE user_id = %s
            ORDER BY tweet_ts DESC
            """,
            (user_id,)
        )
        rows = cursor.fetchall()
        cursor.close()

        tweets = []
        for row in rows:
            tweets.append(
                Tweet(
                    tweet_id=row[0],
                    user_id=row[1],
                    tweet_text=row[2],
                    tweet_ts=row[3]
                )
            )

        return tweets

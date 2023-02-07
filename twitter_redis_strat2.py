import redis
from twitter_objects import Tweet, Follow, User
import time
import pandas as pd

class TwitterAPI:
    def __init__(self, host="localhost", port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db)

    def post_tweet(self, tweet):
        tweet_id = str(self.r.incr('tweetID'))
        user_id = str(tweet.user_id)
        tweet_text = str(tweet.tweet_text)
        tweet_ts = time.time_ns()
        self.r.set(f"tweet:{user_id}:{tweet_id}", f"{tweet_ts}:{tweet_text}")
        for follower in self.r.lrange(f"followers:{user_id}", 0, -1):
            timeline_str = f"timeline:{follower.decode('utf-8')}"
            self.r.zadd(timeline_str, {tweet_text: tweet_ts})

    def get_timeline(self, user_id):
        return self.r.zrange(f"timeline:{user_id}", 0, 9)

    def get_random_user_id(self):
        return self.r.randomkey()
    
    def load_twitter_redis(self):
        self.r.flushall()
        follows = pd.read_csv('data/follows.csv')
        for i in range(len(follows)):
            self.r.lpush(f"followers:{follows.iloc[i][1]}", str(follows.iloc[i][0]))

    def add_follower(self, follower_id, followee_id):
        self.r.lpush(f"followers:{followee_id}", follower_id)
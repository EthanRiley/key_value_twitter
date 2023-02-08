import redis
from twitter_objects import Tweet, Follow, User
import time
import pandas as pd
import random as rnd

class TwitterAPI:
    def __init__(self, host="localhost", port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def post_tweet(self, tweet):
        tweet_id = str(self.r.incr('tweetID'))
        user_id = str(tweet.user_id)
        tweet_text = str(tweet.tweet_text)
        tweet_ts = time.time_ns()
        self.r.set(f"tweet:{tweet_id}", f"{user_id}:{tweet_ts}:{tweet_text}")
        for follower in self.r.lrange(f"followers:{user_id}", 0, -1):
            timeline_str = f"timeline:{follower}"
            self.r.zadd(timeline_str, {tweet_id: tweet_ts})

    def get_timeline(self, user_id):
        ids = self.r.zrange(f"timeline:{user_id}", 0, 9)
        tweets = []
        for id in ids:
            tweet_info = self.r.get(f"tweet:{id}")
            tweet_parts = tweet_info.split(':')
            tweets.append(Tweet(user_id=tweet_parts[0], tweet_text=tweet_parts[2]))
        return tweets
        

    def get_random_user_id(self, num):
        users = self.r.lrange('users', 0, -1)
        users_to_submit = []
        for _ in range(num):
            users_to_submit.append(rnd.choice(users))
        return users_to_submit
    
    def load_twitter_redis(self):
        self.r.flushall()
        follows = pd.read_csv('data/follows.csv')
        # Get all unique user_ids
        user_ids = follows['USER_ID'].unique()
        for i in range(len(follows)):
            self.r.lpush(f"followers:{follows.iloc[i][1]}", str(follows.iloc[i][0]))

        for i in range(len(user_ids)):
            self.r.lpush(f"users", str(user_ids[i]))

    def add_follower(self, follower_id, followee_id):
        self.r.lpush(f"followers:{followee_id}", follower_id)
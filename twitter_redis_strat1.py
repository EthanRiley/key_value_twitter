import redis
from twitter_objects import Tweet, Follow, User
import time
import pandas as pd
import random as rnd

class TwitterAPI:
    def __init__(self, host="localhost", port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def post_tweet(self, tweet):
        tweet_id = self.r.incr('tweetID')
        user_id = tweet.user_id
        tweet_text = tweet.tweet_text
        tweet_ts = time.time_ns()
        self.r.set(f"tweet:{tweet_id}", f"{user_id}:{tweet_ts}:{tweet_text}")
        self.r.lpush(f"tweets:{user_id}", tweet_id)

    def get_timeline(self, user_id):
        following = self.r.lrange(f"following:{user_id}", 0, -1)
        tweets = []
        for followee in following:
            followee_tweets = self.r.lrange(f"tweets:{followee}", -10, -1)
            for tweet_id in followee_tweets:
                tweet = self.r.get(f"tweet:{tweet_id}")
                tweet_parts = tweet.split(':')
                tweet_dict = {'tweet_user_id': tweet_parts[0], 'tweet_ts': tweet_parts[1], 'tweet_text': tweet_parts[2]}
                tweets.append(tweet_dict)
        sorted_tweets = sorted(tweets, key=lambda x: x['tweet_ts'], reverse=True)
        tweets = [Tweet(user_id=tweet['tweet_user_id'], tweet_text=tweet['tweet_text'], tweet_ts=tweet['tweet_ts']) for tweet in sorted_tweets[:10]]
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
            self.r.lpush(f"following:{follows.iloc[i][0]}", str(follows.iloc[i][1]))

        for i in range(len(user_ids)):
            self.r.lpush(f"users", str(user_ids[i]))

    def add_follower(self, follower_id, followee_id):
        self.r.lpush(f"followers:{followee_id}", follower_id)
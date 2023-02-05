import redis
from twitter_objects import Tweet, Follow, User
import time

class TwitterAPI:
    def __init__(self, host="localhost", port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db)

    def post_tweet(self, tweet):
        tweet_id = self.r.incr('tweetID')
        user_id = tweet.user_id
        tweet_text = tweet.tweet_text
        tweet_ts = time.time_ns()
        self.r.set()

    def get_timeline(self, user_id):
        self.r.get(user_id)

    def get_random_user_id(self):
        return self.r.randomkey()
from dbUtils import DBUtils
from twitter_objects import Tweet, Follow, User

class TwitterAPI:
    def __init__(self, user, password, database, host="localhost"):
        self.dbu = DBUtils(user, password, database, host)

    def post_tweet(self, tweet):
        sql = "INSERT INTO tweets (user_id, tweet_text) VALUES (%s, %s)"
        val = (tweet.user_id, tweet.tweet_text)
        self.dbu.insert_one(sql, val)

    def get_timeline(self, user_id):
        sql = f"SELECT tweet_text FROM tweets JOIN follows ON tweets.user_id = follows.user_id WHERE tweets.user_id = {user_id} ORDER BY tweet_ts DESC LIMIT 10;"
        val = (user_id,)
        self.dbu.execute(sql)

    def get_random_user_id(self):
        sql = "SELECT user_id FROM follows ORDER BY RAND() LIMIT 1;"
        df = self.dbu.execute(sql)
        return df.iloc[0][0]
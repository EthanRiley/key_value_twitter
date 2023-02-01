class Tweet:
    
    def __init__(self, user_id, tweet_text):
        self.user_id = user_id
        self.tweet_text = tweet_text

    def __str__(self):
        return f"Tweet: {self.tweet_text} by {self.user_id} on {self.tweet_ts}"

class Follow:

    def __init__(self, follower_id, followee_id):
        self.follower_id = follower_id
        self.followee_id = followee_id

    def __str__(self):
        return f"{self.follower_id} follows {self.followee_id}"

class User:

    def __init__(self, user_id, user_name):
        self.user_id = user_id
        self.user_name = user_name

    def __str__(self):
        return f"User: {self.user_name}"
import pandas as pd
import time
from twitter_mysql import TwitterAPI
from twitter_redis_strat2 import TwitterAPI as TwitterAPIRedis
from twitter_redis_strat1 import TwitterAPI as TwitterAPIRedisStrat1
from twitter_objects import Tweet, Follow, User
# Load tweet.csv 

# Create Tweet objects associated with each line of the csv file
def load_and_profile_tweets():
    # Create API Connection
    api = TwitterAPI('pySQL', 'Python123', 'test_twitter')
    #  Load the csv file
    tweets = pd.read_csv('data/tweet.csv')
    start = time.time_ns()
    time_per_10k = []
    for i in range(len(tweets)):
        if i % 10 ** 4 == 0:
            finish = time.time_ns()
            completion_time = (finish - start) / 10 ** 9
            time_per_10k.append(completion_time)
            start = time.time_ns()
        tweet = Tweet(int(tweets['USER_ID'][i]), tweets['TWEET_TEXT'][i])
        api.post_tweet(tweet)
    api.dbu.close()
    return time_per_10k

def load_and_profile_tweets_redis_strat2():
    api = TwitterAPIRedis()
    tweets = pd.read_csv('data/tweet.csv')
    start = time.time_ns()
    time_per_10k = []
    for i in range(len(tweets)):
        if i % 10 ** 4 == 0:
            finish = time.time_ns()
            completion_time = (finish - start) / 10 ** 9
            time_per_10k.append(completion_time)
            start = time.time_ns()
        tweet = Tweet(int(tweets['USER_ID'][i]), tweets['TWEET_TEXT'][i])
        api.post_tweet(tweet)
    # Find average of time_per_10k
    average = sum(time_per_10k) / len(time_per_10k)
    return 10000 / average

def load_and_profile_tweets_func(strategy):
    api = strategy()
    tweets = pd.read_csv('data/tweet.csv')
    start = time.time_ns()
    time_per_10k = []
    for i in range(len(tweets)):
        if i % 10 ** 4 == 0:
            finish = time.time_ns()
            completion_time = (finish - start) / 10 ** 9
            time_per_10k.append(completion_time)
            start = time.time_ns()
        tweet = Tweet(int(tweets['USER_ID'][i]), tweets['TWEET_TEXT'][i])
        api.post_tweet(tweet)
    # Find average of time_per_10k
    average = sum(time_per_10k) / len(time_per_10k)
    return 10000 / average

def get_timelines_redis_strat2():
    api = TwitterAPIRedis()
    user_ids = api.get_random_user_id(50000)
    start = time.time_ns()
    for id in user_ids:
        tl = api.get_timeline(id)
    finish = time.time_ns()
    time_elapsed = (finish - start) / 10 ** 9
    return 50000 / time_elapsed

def get_timelines_func(strategy):
    api = strategy()
    user_ids = api.get_random_user_id(50000)
    start = time.time_ns()
    for id in user_ids:
        tl = api.get_timeline(id)
    finish = time.time_ns()
    time_elapsed = (finish - start) / 10 ** 9
    return 50000 / time_elapsed

def get_timelines():
    api = TwitterAPI('pySQL', 'Python123', 'test_twitter')
    start = time.time_ns()
    time_elapsed = 0
    total_timelines = 0
    while time_elapsed < 60:
        user_id = api.get_random_user_id()
        api.get_timeline(user_id)
        total_timelines += 1
        finish = time.time_ns()
        time_elapsed = (finish - start) / 10 ** 9
    api.dbu.close()
    return total_timelines

def main():
    # Load and profile tweets
    #time_per_10k = load_and_profile_tweets()
    #print(f'Load and profile tweets: {time_per_10k}')
    # Get timelines
    #total_timelines = get_timelines()
    #print(f'Total timelines: {total_timelines}')

    # Load follower relationships redis strat2

    # Load and profile tweets redis strat 1

    api = TwitterAPIRedisStrat1()
    api.load_twitter_redis()
    time_per_10k = load_and_profile_tweets_func(TwitterAPIRedisStrat1)
    print(f'Load and profile tweets average time redis strat 1: {time_per_10k}')
    # Get timelines redis strat2
    total_timelines = get_timelines_func(TwitterAPIRedisStrat1)
    print(f'Total timelines average time redis strat 1: {total_timelines}')

    
    api = TwitterAPIRedis()
    api.load_twitter_redis()
    # Load and profile tweets redis strat 2
    time_per_10k = load_and_profile_tweets_func(TwitterAPIRedis)
    print(f'Load and profile tweets average time redis strat 2: {time_per_10k}')
    # Get timelines redis strat2
    total_timelines = get_timelines_func(TwitterAPIRedis)
    print(f'Total timelines average time redis strat 2: {total_timelines}')



if __name__ == '__main__':
    main()
    
import pandas as pd
import time
from twitter_mysql import TwitterAPI
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
    time_per_10k = load_and_profile_tweets()
    print(f'Load and profile tweets: {time_per_10k}')
    # Get timelines
    total_timelines = get_timelines()
    print(f'Total timelines: {total_timelines}')

if __name__ == '__main__':
    main()
    
from twitter_redis_strat2 import TwitterAPI
from twitter_objects import Tweet, Follow, User
def main():
    api = TwitterAPI()
    api.load_twitter_redis()
    test_list = list(api.r.lrange('followers:1', 0, -1))
    assert test_list == [b'6908', b'5042', b'7128', b'4079', b'6630']
    print('Test passed!')

    test_tweet1 = Tweet(1, 'Hello world!')
    api.post_tweet(test_tweet1)
    print(api.r.get('tweet:1:1'))
    print(api.get_timeline(6908))
    
    test_tweet2 = Tweet(1, 'Hello world again!')
    api.post_tweet(test_tweet2)
    print(api.get_timeline(6908))


if __name__ == '__main__':
    main()
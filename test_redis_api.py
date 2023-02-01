import redis

def test_connection():
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set('foo', 'bar')
    assert r.get('foo') == 'bar'
from redis import Redis

redis_client = Redis(host='localhost', port=6379, decode_responses=True)
redis_client.set("foo", "bar")
print(redis_client.get("foo"))
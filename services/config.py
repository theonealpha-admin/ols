import os
import redis
import time


# Configure Redis parameters from environment variables (with defaults)
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REALTIME_CACHE_TTL = int(os.getenv("REALTIME_CACHE_TTL", 20))  # TTL for real-time data

# Initialize Redis client with connection pooling and timeout settings
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    socket_timeout=5,  # seconds
    socket_connect_timeout=5
)


def redis_connection(retries=5, delay=2):
    """
    Attempts to ping Redis until a connection is established.
    :param retries: Number of retry attempts.
    :param delay: Delay in seconds between retries.
    :return: A connected Redis client.
    :raises Exception: If unable to connect after all retries.
    """
    attempt = 0
    while attempt < retries:
        try:
            if redis_client.ping():
                return redis_client
        except Exception as e:
            print(f"Error connecting to Redis on attempt {attempt + 1}: {e}")
        attempt += 1
        time.sleep(delay)

    raise Exception("Unable to connect to Redis after multiple attempts.")


def set_with_ttl(key, value, ttl=REALTIME_CACHE_TTL):
    """
    Set a key with a value in Redis and a time-to-live.
    :param key: Redis key.
    :param value: Value to set.
    :param ttl: Time-to-live in seconds.
    """
    redis_client.set(key, value, ex=ttl)


def get_value(key):
    """
    Get the value of a key from Redis.
    :param key: Redis key.
    :return: Value of the key or None if not found.
    """
    return redis_client.get(key)


def add_to_sorted_set(key, score, value):
    """
    Add a value to a sorted set in Redis.
    :param key: Redis sorted set key.
    :param score: Score for sorting.
    :param value: Value to add.
    """
    redis_client.zadd(key, {value: score})


def get_sorted_set_range(key, start, end):
    """
    Retrieve a range of values from a sorted set in Redis.
    :param key: Redis sorted set key.
    :param start: Start index.
    :param end: End index.
    :return: List of values within the range.
    """
    return redis_client.zrange(key, start, end)


def add_to_stream(stream_key, data):
    """
    Add data to a Redis stream.
    :param stream_key: Redis stream key.
    :param data: Dictionary of data to add.
    """
    redis_client.xadd(stream_key, data)


def get_stream_data(stream_key, count=100):
    """
    Retrieve data from a Redis stream.
    :param stream_key: Redis stream key.
    :param count: Number of entries to retrieve.
    :return: List of stream entries.
    """
    return redis_client.xrange(stream_key, count=count)


# if __name__ == "__main__":
#     redis_client = redis_connection()
#     print("Redis connection established.")

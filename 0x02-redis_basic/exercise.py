#!/usr/bin/env python3
import redis
import uuid
from typing import Union, Callable, Optional
from functools import wraps


def count_calls(method: Callable) -> Callable:
    """
    Decorator that counts the number of times a method is called.

    Args:
        method: The method to be decorated.

    Returns:
        The decorated method.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that increments the call count and calls the original method.
        """
        key = method.__qualname__
        self._redis.incr(key)
        return method(self, *args, **kwargs)
    return wrapper

def call_history(method: Callable) -> Callable:
    """
    Decorator that stores the history of inputs and outputs for a particular method.

    Args:
        method: The method to be decorated

    Returns:
        The decorated method
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that stores inputs and outputs in Redis lists and calls the original method.
        """
        input_key = f"{method.__qualname__}:inputs"
        output_key = f"{method.__qualname__}:outputs"
        input_str = str(args)
        self._redis.rpush(input_key, input_str)
        output = method(self, *args, **kwargs)
        self._redis.rpush(output_key, str(output))
        return output
    return wrapper

class Cache:
    """
    Cache class that stores data in Redis.
    """

    def __init__(self):
        """
        Initializes the class with a Redis client and flushes the database.
        """
        self._redis = redis.Redis()
        self._redis.flushdb()


    @count_calls
    @call_history
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Stores data in Redis using a random key and returns the key.

        Args:
            data: The data to be stored in Redis.

        Return:
            The randomly generated key used to store the data.
        """
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key

    def get(self, key: str, fn: Optional[Callable] = None) -> Union[str, bytes, int, float, None]:
        """
        Retrieves data from Redis using the key.

        Args:
            key: The key to retrieve data from Redis.
            fn: An optional callable to convert the data.

        Return:
            - The data retrieved from Redis, converted by the callable if provided, otherwise the raw data.
            - None if the key does not exist.
        """
        value = self._redis.get(key)

        if value is None:
            return None
        if fn is not None:
            return fn(value)

        return value # Return the value (bytes)

    def get_str(self, key: str) -> Optional[str]:
        """
        Retrieves a string from Redis using the key.

        Args:
            key: The key to retrieve data from Redis.

        Returns:
            The string received from Redis, or None if the key does not exist.
        """
        return self.get(key, fn=lambda d: d.decode('utf-8'))

    def get_int(self, key: str) -> Optional[int]:
        """
        Retrieves an integer from Redis using the key.
        """
        return self.get(key, fn=int)

    def replay(self, method: Callable):
        """
        Replays the history of calls for a particular method.

        Args:
            method: The method to replay the history for.
        """
        method_name = method.__qualname__
        input_key = f"{method_name}:inputs"
        output_key = f"{method_name}:outputs"
        inputs = self._redis.lrange(input_key, 0, -1)
        outputs = self._redis.lrange(output_key, 0, -1)

        print(f"{method_name} was called {len(inputs)} times:")
        for i, (input_bytes, output_bytes) in enumerate(zip(inputs, outputs)):
            input_str = input_bytes.decode("utf-8")
            output_str = output_bytes.decode("utf-8")
            print(f"{method_name}(*{input_str}) -> {output_str}")


# Testing the implementation
cache = Cache()

TEST_CASES = {
    b"foo": None,
    123: int,
    "bar": lambda d: d.decode('utf-8'),
}

for value, fn in TEST_CASES.items():
    key = cache.store(value)
    assert cache.get(key, fn=fn) == value


print("All tests passed!")

import redis.asyncio as redis
import os
from dotenv import load_dotenv

load_dotenv()

class RedisManager:
    def __init__(self):
        raw_host = os.getenv('REDIS_HOST')
        host = '127.0.0.1' if raw_host == 'quant-redis' and not self._is_in_docker() else raw_host

        self._redis_client = redis.ConnectionPool(
            host=host,
            port=os.getenv('REDIS_PORT'),
            password=os.getenv('REDIS_PASSWORD'),
            db=0,
            decode_responses=True,
            max_connections=20
        )

    def _is_in_docker(self):
        return os.path.exists('/.dockerenv')

    @property
    def connect(self):
        return redis.Redis(connection_pool=self._redis_client)
    
redis_manager = RedisManager()

    
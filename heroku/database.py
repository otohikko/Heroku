import os
import redis
from urllib.parse import urlparse
from typing import Optional

class Database:
    def __init__(self):
        self._redis: Optional[redis.Redis] = None

    async def redis_init(self) -> bool:
        """Init redis database"""
        REDIS_URI = os.environ.get("REDIS_URL") or self.get_config_key("redis_uri")
        if not REDIS_URI:
            return False

        url = urlparse(REDIS_URI)

        # Railway часто отдаёт rediss://default:PASSWORD@host:port
        if url.username == "default":
            # удаляем default: из строки подключения
            REDIS_URI = REDIS_URI.replace("default:", "", 1)

        # Если TLS (rediss://), включаем SSL
        if url.scheme == "rediss":
            self._redis = redis.Redis.from_url(
                REDIS_URI,
                ssl=True,
                ssl_cert_reqs=None
            )
        else:
            self._redis = redis.Redis.from_url(REDIS_URI)

        try:
            # Проверка соединения
            self._redis.ping()
        except redis.exceptions.AuthenticationError as e:
            print(f"[ERROR] Redis auth failed: {e}")
            return False
        except redis.exceptions.ConnectionError as e:
            print(f"[ERROR] Redis connection failed: {e}")
            return False

        return True

    def get_config_key(self, key: str) -> Optional[str]:
        # Заглушка, замени на свой способ получения из конфига
        return None

    def read(self, key: str):
        if not self._redis:
            raise RuntimeError("Redis is not initialized")
        return self._redis.get(key)

    def write(self, key: str, value: str):
        if not self._redis:
            raise RuntimeError("Redis is not initialized")
        self._redis.set(key, value)
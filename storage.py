import json
from typing import Any, Optional

from teems.nordpoolservice.config import NORDPOOL_CONFIG

from common.protocols.redis_ import RedisProtocol
from common.redis_ import get_redis_client


class NordPoolRedisStorage:
    def __init__(self, redis: Optional[RedisProtocol] = None) -> None:
        self.redis_client = redis if redis is not None else get_redis_client(decode_responses=True)


class NordPoolTokenStorage(NordPoolRedisStorage):
    def __init__(self, redis: Optional[RedisProtocol] = None):
        super().__init__()
        self.key = NORDPOOL_CONFIG.token_storage_key

    async def save_token(self, key: str, value) -> Any:
        return await self.redis_client.hset(self.key, key, value.json())

    async def get_token(self, key: str) -> Any:
        try:
            return json.loads(await self.redis_client.hget(self.key, key))
        except TypeError:
            return None

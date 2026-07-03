from src.storage.redis.client import redis_manager

import asyncio
from logging import Logger

class RedisStreamKeys:
    def __init__(self):
        self.redis = redis_manager.connect

    async def get_stream_keys(self,group_name:str,data_types:list,streamings:dict,logger:Logger):
        for data_type in data_types:
            registry = f"registry:streams:{data_type}"
            keys = await self.redis.smembers(registry)
            
            if keys:
                for key in keys:
                    if key not in streamings:
                        try:
                            await self.redis.xgroup_create(
                                name=key,
                                groupname=group_name,
                                id="0",
                                mkstream=True
                            )
                        except Exception as e:
                            if "BUSYGROUP" not in str(e):
                                logger.error(f"creat group error: {e}")
                        finally:
                            streamings[key] = ">"

        return streamings
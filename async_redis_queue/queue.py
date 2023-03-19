from typing import Union

import aioredis
import ujson
from pydantic import BaseModel


class Queue:
    def __init__(
        self,
        name,
        url="redis://localhost:6379",
        model: BaseModel = None,
        timeout=0,
    ):
        self.name = name
        self.timeout = timeout
        self.model = model

        self.client = aioredis.from_url(url, decode_responses=True)

    async def push(self, item: Union[str, BaseModel]):
        if self.model is not None:
            item = item.json()

        await self.client.lpush(self.name, item)

    async def pop(self):
        _, item = await self.client.brpop(self.name, timeout=self.timeout)
        if self.model is not None:
            item = ujson.loads(item)
            item = self.model(**item)

        return item

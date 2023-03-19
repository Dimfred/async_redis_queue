import asyncio as aio

from pydantic import BaseModel

from .queue import Queue


class Item(BaseModel):
    a: str
    b: str


queue = Queue("test", model=Item)


async def pusher():
    item = Item(a="a", b="b")

    while True:
        await queue.push(item)
        await aio.sleep(1)
        await queue.push(item)
        await aio.sleep(1)


async def main():
    aio.create_task(pusher())
    await aio.sleep(3)
    while True:
        res = await queue.pop()
        print(res)


aio.run(main())

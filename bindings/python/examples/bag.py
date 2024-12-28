import asyncio
import streamstore as s2
import time


async def create_basin():
    cfg = s2.ClientConfig(auth_token="<AUTH_TOKEN>")
    c = s2.Client(cfg)
    res = await c.delete_basin("s2-sdk-py")
    print(res)
    print("sleeping for 2 mins")
    time.sleep(120)
    b = await c.create_basin("s2-sdk-py")
    print(b)


if __name__ == "__main__":
    asyncio.run(create_basin())

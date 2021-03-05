#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

from aredis import StrictRedisCluster
from aredis import StrictRedis
from aredis import ConnectionError
import logging
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class RedisClient:
    def __init__(self, host, port, password, cluster=True):
        '''
        用于初始化项目测试，不需要任何参数
        :return: 
        '''
        self.host = host
        self.port = port
        self.password = password
        self.client = None
        self.cluster = cluster

    def get_client(self):
        return self.client

    def connect_to_redis(self):
        """
        connect to redis 成功 失败
        """
        try:
            if self.cluster != True:
                self.client = StrictRedis(host=self.host,
                                        port=self.port,
                                        decode_responses=True,
                                        password=self.password,
                                        socket_keepalive=True,
                                        max_idle_time=32,
                                        max_connections=512)
            else:
                self.client = StrictRedisCluster(host=self.host,
                                                port=self.port,
                                                decode_responses=True,
                                                password=self.password,
                                                socket_keepalive=True,
                                                max_idle_time=32,
                                                max_connections=512)
        except Exception as e:
            logger.error(f"connect to redis exception:{e}")
            logger.error("trace:" + traceback.format_exc())
            return -1
        return 0

    async def exists(self, key, retry=False):
        ret = 0
        try:
            # logger.debug("redis exists key:{} ".format(key))
            if await self.client.exists(key):
                return 0
            else:
                return 1
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.exists(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis exists exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.exists(key, retry=False)
            return -1

    async def delete(self, key, retry=False):
        try:
            logger.debug("redis del key:{}".format(key))
            await self.client.delete(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.delete(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis del exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.delete(key, retry=False)
            return -1
        return 0

    async def set(self, key, value, ex=None, px=None, retry=False, nx=False):
        try:
            logger.debug("redis set key:{} value:{}".format(key, value))
            if True != await self.client.set(key, value, ex=ex, px=px, nx=nx):
                if nx != False:
                    logger.error("!!! redis set return false !!!")
                return -1
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.set(key, value, ex=ex, px=px, retry=False, nx=nx)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis set key exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.set(key, value, ex=ex, px=px, retry=False, nx=nx)
            return -1
        return 0

    async def get(self, key, retry=False):
        value = ""
        ret = 0
        try:
            value = await self.client.get(key)
            logger.debug("redis get key:{} value:{}".format(key, value))
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.get(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis get key exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.get(key, retry=False)
        return (ret, value)

    async def sadd(self, key, value, retry=False):
        try:
            logger.debug("redis sadd key:{} value:{}".format(key, value))
            await self.client.sadd(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.sadd(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis sadd exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.sadd(key, value, retry=False)
            return -1
        return 0

    async def srem(self, key, value, retry=False):
        try:
            logger.debug("redis srem key:{} value:{}".format(key, value))
            await self.client.srem(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.srem(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis srem:{e} exception key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.srem(key, value, retry=False)
            return -1
        return 0

    async def get_members(self, key, retry=False):
        ret = 0
        keys = set()
        try:
            logger.debug("redis get members:{}".format(key))
            keys = await self.client.smembers(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.get_members(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis get members exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.get_members(key, retry=False)
        return (ret, keys)

    async def scard(self, key, retry=False):
        ret = 0
        nums = -1
        try:
            logger.debug("redis get set:{} nums".format(key))
            nums = await self.client.scard(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.scard(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis scard exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.scard(key, retry=False)
        return (ret, nums)

    async def sscan(self, key, cursor=0, match=None, count=None, retry=False):
        ret = 0
        nums = list()
        try:
            logger.debug("redis sscan:{}".format(key))
            data = await self.client.sscan(key, cursor=cursor, match=match, count=count)
            ret = data[0]
            nums = data[1]
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.sscan(key, cursor=cursor, match=match, count=count, retry=False)
            else:
                return (ret, nums)
        except Exception as e:
            logger.error(f"redis sscan exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.sscan(key, cursor=cursor, match=match, count=count, retry=False)
        return (ret, nums)

    async def publish(self, key, value, retry=False):
        try:
            logger.debug("redis publish key:{} value:{}".format(key, value))
            await self.client.publish(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.publish(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis publish exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.publish(key, value, retry=False)
            return -1
        return 0

    async def eval(self, script, numkeys, *keys_and_args, retry=False):
        ret = 0
        try:
            logger.debug("redis eval script:{}".format(script))
            if not await self.client.eval(script, numkeys, *keys_and_args):
                ret = -1 
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.eval(script, numkeys, *keys_and_args, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis eval exception:{e} script:{script}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.eval(script, numkeys, *keys_and_args, retry=False)
            return -1
        return ret


    async def llen(self, key, retry=False):
        ret = 0
        try:
            logger.debug("redis llen key:{}".format(key))
            ret = await self.client.llen(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.llen(key, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis llen exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.llen(key, retry=False)
            return -1
        return ret

    async def rpush(self, key, value, retry=False):
        llen = 0
        try:
            logger.debug("redis rpush key:{} value:{}".format(key, value))
            llen = await self.client.rpush(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.rpush(key, value, retry=False)
            else:
                return -1, llen
        except Exception as e:
            logger.error(f"redis rpush exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.rpush(key, value, retry=False)
            return -1, llen
        return 0, llen

    async def rpushx(self, key, value, retry=False):
        try:
            logger.debug("redis rpushx key:{} value:{}".format(key, value))
            await self.client.rpushx(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.rpushx(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis rpushx exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.rpushx(key, value, retry=False)
            return -1
        return 0

    async def lpush(self, key, value, retry=False):
        try:
            logger.debug("redis lpush key:{} value:{}".format(key, value))
            await self.client.rpush(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.lpush(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis lpush exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.lpush(key, value, retry=False)
            return -1
        return 0

    async def lpushx(self, key, value, retry=False):
        try:
            logger.debug("redis lpushx key:{} value:{}".format(key, value))
            await self.client.rpushx(key, value)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.lpushx(key, value, retry=False)
            else:
                return -1
        except Exception as e:
            logger.error(f"redis lpushx exception:{e} key:{key} value:{value}")
            logger.error("trace:" + traceback.format_exc())
            if retry != False:
                self.connect_to_redis()
                return await self.lpushx(key, value, retry=False)
            return -1
        return 0

    async def rpop(self, key, retry=False):
        value = None
        ret = 0
        try:
            logger.debug("redis rpop key:{}".format(key))
            value = await self.client.rpop(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.rpop(key, retry=False)
            else:
                return (-1, value)
        except Exception as e:
            logger.error(f"redis rpop exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.rpop(key, retry=False)
        if value is not None and len(value) > 1:
            return (ret, value[1])
        else:
            return (ret, value)

    async def brpop(self, key, timeout=0, retry=False):
        value = None
        ret = 0
        try:
            logger.debug("redis brpop key:{}".format(key))
            value = await self.client.brpop(key, timeout)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.brpop(key, timeout, retry=False)
            else:
                return (-1, value)
        except Exception as e:
            logger.error(f"redis brpop exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.brpop(key, timeout, retry=False)
        if value is not None and len(value) > 1:
            return (ret, value[1])
        else:
            return (ret, value)

    async def lpop(self, key, retry=False):
        value = None
        ret = 0
        try:
            logger.debug("redis lpop key:{}".format(key))
            value = await self.client.lpop(key)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.lpop(key, retry=False)
            else:
                return (-1, value)
        except Exception as e:
            logger.error(f"redis lpop exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.lpop(key, retry=False)
        if value is not None and len(value) > 1:
            return (ret, value[1])
        else:
            return (ret, value)

    async def blpop(self, key, timeout=0, retry=False):
        value = None
        ret = 0
        try:
            logger.debug("redis blpop key:{}".format(key))
            value = await self.client.blpop(key, timeout)
        except ConnectionError:
            if retry != False:
                self.connect_to_redis()
                return await self.blpop(key, timeout, retry=False)
            else:
                return (-1, value)
        except Exception as e:
            logger.error(f"redis lpop exception:{e} key:{key}")
            logger.error("trace:" + traceback.format_exc())
            ret = -1
            if retry != False:
                self.connect_to_redis()
                return await self.blpop(key, timeout, retry=False)
        if value is not None and len(value) > 1:
            return (ret, value[1])
        else:
            return (ret, value)

    async def lock(self, key, value, ex=3):
        return await self.set(key, value, ex=ex, nx=True)

    async def unlock(self, key, value):
        script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
        return await self.eval(script, 1, key, value)



class PubSub:
    def __init__(self, client):
        self.pubsub = client.get_client().pubsub()
        self.thread = None

    async def subscribe(self, *args, **kwargs):
        return await self.pubsub.subscribe(*args, **kwargs)

    def run_in_thread(self, daemon=False, poll_timeout=1.0):
        self.thread = self.pubsub.run_in_thread(daemon, poll_timeout)

    def stop(self):
        if not thread:
            self.thread.stop()



if __name__ == "__main__":
    import asyncio
    import json

    async def test():
        redis_client = RedisClient(
            "127.0.0.1",
            "6379",
            "",
        )
        redis_client.connect_to_redis()

        # ret = await redis_client.get("123")
        # print(ret)


        # ret = await redis_client.sadd("123", "123")
        # ret = await redis_client.sadd("123", "123")
        # print(ret)
        data = json.dumps({"a":"a", "b":1})
        ret = await redis_client.rpush("g_redis_mprocess_task_list", data)

        ret, body = await redis_client.blpop("g_redis_mprocess_task_list", timeout=3)
        info = json.loads(body)
        print(info)


    g_event_loop = asyncio.get_event_loop()
    g_event_loop.run_until_complete(test())
    # g_event_loop.run_until_complete(asyncio.wait(tasks))
    g_event_loop.close()

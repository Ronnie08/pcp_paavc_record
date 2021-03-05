#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

from aredis import StrictRedisCluster
import logging
import traceback
import json
import asyncio
import time
import socket
import traceback
from aiohttp import web
import aiohttp
import aredis
# import signal

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',filename="./demo_test.log",level=logging.INFO)
#logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',level=logging.DEBUG)

# msg_queue_publish = []

demo_ip = "0.0.0.0"
demo_port = 9999
demo_redis_ip = "rdjac12s.redis.db.cloud.papub"
demo_redis_port = 10716
demo_redis_passwd = 'Admin20190517'
demo_url_path = "/test"
demo_key = "demo_key"
demo_key_len = 128

# loop = asyncio.get_event_loop()

class RedisClient:
    def  __init__(self,host,port,password):
        self.host = host
        self.port = port 
        self.password = password
        self.client = None

    def get_client(self):
        return self.client

    def connect_to_redis(self):
        try:
            #self.client = StrictRedisCluster(host='rd7p2351.redis.db.cloud.papub', port=10732,password="Admin20190710",decode_responses=True,socket_keepalive=True)
            self.client = StrictRedisCluster(host=self.host, port=self.port,decode_responses=True,password=self.password,socket_keepalive=True,max_idle_time=30,max_connections=128)
        except Exception as e:
            logging.error("connect to redis  exception trace: " + traceback.format_exc())
            logging.error("connect to redis exception:%s" % e)
            return -1
        
        return 0
    
    async def delete(self,key):
        try:
            logging.info("redis del key:{} ".format(key))
            await self.client.delete(key)
        except Exception as e:
            logging.error("redis set del  exception trace: " + traceback.format_exc())
            logging.error("redis set del  exception:%s" % e)
            return -1
        return 0

    async def set(self,key,value):
        try:
            logging.info("redis set key:{}  value:{} ".format(key,value))
            await self.client.set(key,value)
        except Exception as e:
            logging.error("redis set key  exception trace: " + traceback.format_exc())
            logging.error("redis set key  exception:%s" % e)
            return -1
        return 0
    
    async  def  get(self,key):
        value = ""
        ret = 0
        try:
            value = await self.client.get(key)
            logging.info("redis get key:{}  value:{} ".format(key,value))
        except Exception as e:
            logging.error("redis set key  exception trace: " + traceback.format_exc())
            logging.error("redis set key  exception:%s" % e)
            ret = -1
        return (ret,value)

    async def sadd(self,key,value):
        try:
            logging.info("redis sadd key:{}  value:{} ".format(key,value))
            await self.client.sadd(key,value)
        except Exception as e:
            logging.error("redis sadd key  exception trace: " + traceback.format_exc())
            logging.error("redis sadd key  exception:%s" % e)
            return -1
        return 0
    
    async def srem(self,key,value):
        try:
            logging.info("redis srem key:{}  value:{} ".format(key,value))
            await self.client.srem(key,value)
        except Exception as e:
            logging.error("redis srem key  exception trace: " + traceback.format_exc())
            logging.error("redis srem key  exception:%s" % e)
            return -1
        return 0
    
    async def get_members(self,key):
        ret = 0
        keys = set()
        try:
            logging.info("redis set key:{} ".format(key))
            keys = await self.client.smembers(key)
        except Exception as e:
            logging.error("redis set key  exception trace: " + traceback.format_exc())
            logging.error("redis set key  exception:%s" % e)
            ret = -1
        return (ret,keys)
    

    async def publish(self,key,value):
        return await self.client.publish(key,value)  
  
redis_client = RedisClient(demo_redis_ip,demo_redis_port,demo_redis_passwd)
redis_client.connect_to_redis()



async def init():
    test_value = ""
    for i in range(demo_key_len):
        test_value += "0"
    await redis_client.set(demo_key, test_value)
    ret, value = await redis_client.get(demo_key)
    print('redis_client.get: %s %s' % (ret,value))

async def live_scheduler_handle(request):
    try:
        ret, value = await redis_client.get(demo_key)
        # print('redis_client.get: %s %s' % (ret,value))
        # body = await request.json()
        # logging.info('Request body: %s ' % body)
        # return web.Response(text=json.dumps(response_info))
        return web.Response(text="0")
    except Exception as e:
        logging.error('live_scheduler_handle get an  exception:%s' % e)
        logging.error("live_scheduler_handle  exception trace: " + traceback.format_exc())
        return web.Response(status=404)


async def logger_factory(app,handler):
    async def logger(request):
        logging.info('Request: %s %s' % (request.method,request.path))
        return await handler(request)
    return logger



loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait([init()]))

app = web.Application(middlewares=[logger_factory])
app.add_routes([web.post(demo_url_path, live_scheduler_handle),
                web.get(demo_url_path, live_scheduler_handle)])

if __name__ == '__main__':
    web.run_app(app, host=demo_ip, port=demo_port)


# async def live_scheduler_server():
#     test_value = ""
#     for i in range(demo_key_len):
#         test_value += "0"
#     await redis_client.set(demo_key, test_value)
#     ret, value = await redis_client.get(demo_key)

#     app = web.Application(loop=loop,middlewares=[logger_factory])
#     app.add_routes([web.post(demo_url_path,live_scheduler_handle)])
#     app.add_routes([web.get(demo_url_path,live_scheduler_handle)])
#     app_runner = web.AppRunner(app)
#     await app_runner.setup()
#     srv = await loop.create_server(app_runner.server,demo_ip, demo_port)
#     logging.info('live_scheduler_server started!')
#     return srv        


# if __name__ == '__main__':
#     redis_client = RedisClient(demo_redis_ip,demo_redis_port,demo_redis_passwd)
#     redis_client.connect_to_redis()

#     tasks=[]
#     tasks.append(live_scheduler_server())
#     try:
#         loop.run_until_complete(asyncio.wait(tasks))
#     except KeyboardInterrupt:
#         logging.critical('<Got Signal: SIGINT, shutting down.>')

#     print("===out===\n")
#     # asyncio.run(main())    
#     # main()
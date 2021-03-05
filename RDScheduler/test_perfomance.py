#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import json
import aiohttp
import sys
import asyncio
import logging
from aiohttp import web

loop = asyncio.get_event_loop()
tasks = []

demo_ip = "0.0.0.0"
demo_port = 9999
demo_redis_ip = "rdjac12s.redis.db.cloud.papub"
demo_redis_port = 10716
demo_redis_passwd = 'Admin20190517'
demo_url_path = "/test"
demo_key = "demo_key"
demo_key_len = 128

msg_queue = asyncio.Queue()
count = 0

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(filename)s-%(lineno)d-%(funcName)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Log等级总开关


# logging.basicConfig(
#     format='%(asctime)s - %(levelname)s - %(filename)s-%(lineno)d-%(funcName)s - %(message)s',
#     #   filename=scheduler_configs.scheduler.scheduler_log_file,
#     level=logging.INFO,
# )
async def logger_factory(app, handler):

    async def log(request):
        logger.info("Request: %s %s" % (request.method, request.path))
        return await handler(request)

    return log


async def demo_handle(request):
    try:
        # ret, value = await redis_client.get(demo_key)
        global count
        count += 1
        # await msg_queue.put(count)
        # if count % 1000 == 0:
        #     logger.info("count:{}".format(count))
        for i in range(10000):
            x = i
            pass
        return web.Response(text="0")
    except Exception as e:
        logger.error('get an exception:%s' % e)
        logger.error("exception trace:" + traceback.format_exc())
        return web.Response(status=404)


async def live_query_server():
    """
    启动查询服务
    """
    app = web.Application(loop=loop, middlewares=[logger_factory])  #
    app.add_routes([web.post(demo_url_path, demo_handle), web.get(demo_url_path, demo_handle)])
    app_runner = web.AppRunner(app)
    await app_runner.setup()
    srv = await loop.create_server(
        app_runner.server,
        demo_ip,
        demo_port,
    )
    logger.info("live_query_server started!")
    return srv


async def demo_runner(sever_index=0):
    logger.info("%d start." % sever_index)
    while True:
        try:
            count = await msg_queue.get()
            if count % 1000 == 0:
                logger.info("count:{}".format(count))
        except Exception as e:
            logger.error("stop_runner get an  exception:%s" % e)
            logger.error("stop_runner exception trace: " + traceback.format_exc())

    logger.error("stop_runner %d leave." % sever_index)


if __name__ == '__main__':
    # action = sys.argv[1]
    # taskid = sys.argv[2]
    # output_stream = sys.argv[3]

    tasks.append(live_query_server())
    tasks.append(demo_runner(0))
    loop.run_until_complete(asyncio.wait(tasks))

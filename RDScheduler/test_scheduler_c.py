#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import json
import aiohttp
import sys
import asyncio
import logging
import config
from config import scheduler_configs

logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()
tasks = []


async def send_request_data(url, data, header=False):
    try:
        logger.debug(url)
        logger.debug(data)
        http_headers = {}
        if header:
            http_headers = {scheduler_configs.business.header_key: scheduler_configs.business.header_value, "Content-Type": "application/x-www-form-urlencoded"}
        async with aiohttp.ClientSession() as session:
            # timeout = aiohttp.ClientTimeout(total=30)
            async with session.post(url, data=data, headers=http_headers, timeout=30) as resp:
                status = resp.status
                text = await resp.text()
                logger.info("Response status: {} data: {}".format(status, text))
                return (status, text)
    except Exception as e:
        logger.error('send request json exception:%s' % e)
        logger.error("exception trace:" + traceback.format_exc())
        return (-1, '')


async def send_request(url, json_data):
    print(url)
    async with aiohttp.ClientSession() as session:
        print(json_data)
        async with session.post(url, json=json_data) as resp:
            status = resp.status
            text = await resp.text()
            print("http status:{} text:{}".format(status, text))
            return (status, text)


async def make_request(action, taskid, output_stream):
    request = {}
    request['taskId'] = taskid
    request['appId'] = '61a9e4e0e80ae3ed84ab281611c1822e'
    request['appKey'] = '04001b9d37e355054afe6226d75ea027'
    request['domain'] = 'push-live-product4.avc.itp-live.pingan.com.cn'
    request['appName'] = 'live'
    request['vhost'] = 'vhost1'
    request['streamName'] = 'stream'
    request['startTime'] = 1572251904824
    #request['startTime'] = 1572251373612
    request['endTime'] = 1572251914452
    #request['endTime'] = 1572251903658
    request['fTaskId'] = ''
    status, text = await send_request(
        'http://127.0.0.1:9091/live/record/proxy', request)


async def test_heartbeat():
    while True:
        print("=========test_heartbeat=========")
        data = {
            "action": "on_heartbeat",
            "ip": "127.0.0.1",
            "port": "19000",
            "device_id": "test-srs-device2",
            "vhosts": ["test"],
            "nb_streams": 0,
        }
        status, text = await send_request(
            'http://10.10.112.2:9090/live/scheduler/streams', data)
        await asyncio.sleep(5)


async def test_publish_request():
    await asyncio.sleep(1)
    print("=========test_publish_reques=========")
    data = {
        "action": "on_publish",

        "ip": "127.0.0.1",
        "port": "19000",
        "vhost": "test",
        "appName": "testapp",
        "streamName": "teststreamname",
    }
    status, text = await send_request(
        'http://10.10.112.2:9090/live/scheduler/streams', data)


async def test_business_request():
    print("=========test_business_request=========")
    data = "aType=3&domain=rtmp-live-union.avc.itp-live.pingan.com.cn&vhost=union1&appName=live&streamName=a1_11_SD&time=1595298603632"
    url = "http://10.10.112.188:10100/live/api/inner/stream/callback"
    status, text = await send_request_data(url, data, header=True)
    if -1 == status:
        print("business.url_path:{} error.".format(url))


if __name__ == '__main__':
    # action = sys.argv[1]
    # taskid = sys.argv[2]
    # output_stream = sys.argv[3]
    # tasks.append(make_request(action,taskid,output_stream))
    # tasks.append(test_heartbeat())
    # tasks.append(test_publish_request())
    tasks.append(test_business_request())
    loop.run_until_complete(asyncio.wait(tasks))

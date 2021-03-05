#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import json
import sys
import asyncio
import logging
from record_common import pes_get_configmap_str, pes_get_configmap_int, send_request_json, send_request_params, dict_to_Dict

logging.basicConfig(
    level=logging.WARNING,
    format='[%(asctime)s-%(filename)s-%(lineno)d]:%(levelname)s: %(message)s',  # -%(funcName)s
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
loop = asyncio.get_event_loop()
tasks = []


async def test_mprocess():

    # task_info = {
    #     "action": "CreateMediaProcessTask",
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611212753626 - 280000,
    #     "domain": "www.test.com",
    #     "appName": "live",
    #     "streamName": "ok",
    #     "objectKey": "",
    #     "target": "IOBS-IOT",
    #     "host": "stg-iobs-upload.pingan.com.cn",
    # }

    # task_info = {
    #     "action": "CreateMediaProcessTask",
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611212753626 - 280000,
    #     "domain": "www.test.com",
    #     "appName": "live",
    #     "streamName": "ok",
    #     "objectKey": "",
    #     "target": "OBS",
    #     "host": "obs-cn-shenzhen.yun.pingan.com",
    #     "bucketName": "paavc-vod-publictest4",
    #     "accessKey": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
    #     "secretKey": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
    #     "token": "",
    # }

    task_info = {
        "action": "CreateMediaProcessTask",
        "startTime": 1614332280000,
        "endTime": 1614332300000,
        "domain": "push.localhost",
        "appName": "live",
        "streamName": "ok02265",
        "objectKey": "",
        "target": "OBS",
        #"host": "obs-cn-shenzhen.yun.pingan.com",
        "host": "obs-cn-shenzhen-internal.cloud.papub",
        "bucketName": "paavc-vod-publictest4",
        "accessKey": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
        "secretKey": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
        "token": "",

        "mprocess_task_id": "123"
    }

    status, text = await send_request_json('http://127.0.0.1:18080/scheduler/mprocess/v1', json_data=task_info)
    # status, text = await send_request_json('http://127.0.0.1:8000/record/live/mprocess', json_data=task_info)
    logger.info(f"status:{status} text:{text}")


if __name__ == '__main__':
    # action = sys.argv[1]
    # taskid = sys.argv[2]
    # output_stream = sys.argv[3]
    # tasks.append(make_request(action,taskid,output_stream))
    # tasks.append(test_heartbeat())
    # tasks.append(test_publish_request())
    # tasks.append(test_mprocess())
    # tasks.append(test_mprocess())
    # tasks.append(test_mprocess())
    # tasks.append(test_mprocess())
    for _ in range(1):
        tasks.append(test_mprocess())
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

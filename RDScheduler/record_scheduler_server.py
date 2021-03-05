#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import os
import time
import socket
import traceback
import random
import copy
import signal
import asyncio
import json
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
import hashlib

from aiohttp import web
from aredis_client import RedisClient
from aredis_client import PubSub
from record_common import pes_get_configmap_str, pes_get_configmap_int, send_request_json, send_request_params, dict_to_Dict
import record_scheduler_config as scheduler_settings
from importlib import reload

g_settings = dict_to_Dict(scheduler_settings.configs)  # 服务启动配置
logging.basicConfig(
    level=logging.WARNING,
    format='[%(asctime)s-%(filename)s-%(lineno)d]:%(levelname)s: %(message)s',  # -%(funcName)s
    handlers=[logging.StreamHandler(),
              TimedRotatingFileHandler(filename=g_settings.server.log_file, when="MIDNIGHT", interval=1, backupCount=7)]  # S M H D MIDNIGHT
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# redis 统一key值资源
g_redis_prefix = "pard|{}|".format(g_settings.server.cluster_id)
g_redis_key_expire = 15 * 24 * 3600  # 存储节点 属性JSON数据 默认有效期
g_redis_gwnode_keys = g_redis_prefix + "gwnode_keys"  # 接收节点名称 集合
g_redis_gwnode_prefix = g_redis_prefix + "gwnode|"  # 网关接收节点 属性JSON数据
g_redis_rdnode_keys = g_redis_prefix + "rdnode_keys"  # 存储节点名称 集合
g_redis_rdnode_prefix = g_redis_prefix + "rdnode|"  # 存储节点 属性JSON数据
g_redis_upnode_keys = g_redis_prefix + "upnode_keys"  # 存储节点名称 集合
g_redis_upnode_prefix = g_redis_prefix + "upnode|"  # 存储节点 属性JSON数据
g_redis_mpnode_keys = g_redis_prefix + "mpnode_keys"  # 存储节点名称 集合
g_redis_mpnode_prefix = g_redis_prefix + "mpnode|"  # 存储节点 属性JSON数据
g_redis_subscribe_channel = g_redis_prefix + "channel"  # 订阅通道
g_redis_subscribe_node_type = g_redis_prefix + "type"  # 订阅通道，接收到新的节点
g_redis_subscribe_gwnode_type = g_redis_prefix + "new_gwnode"  # 订阅通道，接收新的 网关接收节点 通知 type
g_redis_subscribe_rdnode_type = g_redis_prefix + "new_rdnode"  # 订阅通道，接收新的 录制节点 通知 type
g_redis_subscribe_upnode_type = g_redis_prefix + "new_upnode"  # 订阅通道，接收新的 录制节点 通知 type
g_redis_subscribe_mpnode_type = g_redis_prefix + "new_mpnode"  # 订阅通道，接收新的 录制节点 通知 type
# g_redis_subscribe_unpublish = g_redis_prefix + "unpublish"  # 订阅通道，接收流结束通知
# g_redis_subscribe_pstream = g_redis_prefix + "pstream"  # 订阅通道，接收流更新通知
# g_redis_subscribe_rstream = g_redis_prefix + "rstream"  # 订阅通道，录制流更新通知
g_redis_gateway_prefix = g_redis_prefix + "gw_"  # 所有推流名称 集合
g_redis_record_prefix = g_redis_prefix + "rd_"  # 所有录制名称 集合
g_redis_mprocess_task_list = g_redis_prefix + "mprocess_tlist"  # 录制任务队列名称

# 缺省域名
g_default_domain_name = "push.localhost"

# 默认集群名称
g_cluster_default_name = "Default_Cluster"
# 心跳服务器类型
g_ntype_name_gateway = "rd_gateway"
g_ntype_name_record = "rd_record"
g_ntype_name_upload = "rd_upload"
g_ntype_name_mprocess = "rd_mprocess"
# 心跳节点信息
g_node_gateway_dict = {}
g_node_record_dict = {}
g_node_upload_dict = {}
g_node_mprocess_dict = {}

g_event_loop = asyncio.get_event_loop()
g_msg_queue_trace_req = asyncio.Queue()
g_msg_queue_heartbeat = asyncio.Queue()
g_msg_queue_mprocess_cbk = asyncio.Queue()
g_redis_client = None
g_wait_upload_tasks = {}


def pes_update_config():
    pass
    # value = pes_get_configmap_str("/tmp/upload/temp_dir")
    # if value is not None:
    #     configs.server.temp_dir = value

    # value = pes_get_configmap_int("/tmp/upload/temp_dir_del_MB")
    # if value is not None:
    #     configs.server.temp_dir_del_MB = value


async def _api_logger_factory(app, handler):
    async def log(request):
        # logger.info("Request: %s %s" % (request.method, request.path))
        return await handler(request)

    return log


async def task_load_info_from_redis():
    """
    加载节点信息，数据缓存本地，加快搜索速度
    """
    logger.info("start task_load_info_from_redis")
    while True:
        try:
            now_time = time.time()

            gw_nstreams = 0
            rd_nstreams = 0
            for redis_prefix, redis_node_keys, nodes_dict in [(g_redis_gwnode_prefix, g_redis_gwnode_keys, g_node_gateway_dict),
                                                              (g_redis_rdnode_prefix, g_redis_rdnode_keys, g_node_record_dict),
                                                              (g_redis_upnode_prefix, g_redis_upnode_keys, g_node_upload_dict),
                                                              (g_redis_mpnode_prefix, g_redis_mpnode_keys, g_node_mprocess_dict)]:
                ret, node_keys = await g_redis_client.get_members(redis_node_keys, retry=True)
                for node in node_keys:
                    node_key = node.replace(redis_prefix, "")
                    if node_key not in nodes_dict:
                        ret, value = await g_redis_client.get(node, retry=True)
                        if ret == 0 and value is not None and len(value) > 0:
                            msg = json.loads(value)
                            # 排除历史记录的无效节点
                            if (now_time - msg["update_time"] <= 2 * g_settings.server.heartbeat_interval):
                                nodes_dict[node_key] = msg
                                logger.warning(f"find new node:{msg}")
                            else:
                                logger.warning(f"skip timeout node:{msg}")
                                await g_redis_client.srem(redis_node_keys, node)
                        else:
                            logger.warning(f"node:{node} load info from redis error!")
                    else:
                        if redis_prefix == g_redis_gwnode_prefix:
                            gw_nstreams += nodes_dict[node_key]["nb_streams"]
                            pass
                        elif redis_prefix == g_redis_rdnode_prefix:
                            rd_nstreams += nodes_dict[node_key]["nb_streams"]

            count = await g_redis_client.llen(g_redis_mprocess_task_list)
            logger.info("*** loop check gateway node count:{} nstreams:{}".format(len(g_node_gateway_dict), gw_nstreams))
            logger.info("*** loop check record node count:{} nstreams:{}".format(len(g_node_record_dict), rd_nstreams))
            logger.info("*** loop check upload node count:{}".format(len(g_node_upload_dict)))
            logger.info("*** loop check mprocess node count:{} mtasks:{}".format(len(g_node_mprocess_dict), count))
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        # 轮询检查，如果有丢失的/未检测到的新增节点
        await asyncio.sleep(300)


async def task_handle_async_api_heartbeat(sever_index=0):
    """
    {
        "action" : "Heartbeat"
        通用信息
        "cluster_id" : ""
        "ntype" : "rd_gateway/rd_record/rd_upload/rd_media"
        "device_id" : ""
        "ip" : ""
        "port" : ""

        特定节点信息
        "ntype" : "rd_gateway/rd_record"
        "api_port" : ""
        "nb_streams" : ""
        "vhosts" : ""

        "ntype" : "rd_upload"

        "ntype" : "rd_mprocess"
        "process_count": 10,
        "upload_count": 10 
    }
    """
    logger.info("task_handle_async_api_heartbeat start.")
    while True:
        try:
            msg = await g_msg_queue_heartbeat.get()
            # logger.debug("heartbeat msg:{}".format(msg))
            current_time = time.time()
            ntype = msg.get("ntype")
            if ntype not in [g_ntype_name_gateway, g_ntype_name_record, g_ntype_name_upload, g_ntype_name_mprocess]:
                logger.warning(f"heartbeat unknow msg:{ntype}")
                continue

            redis_prefix = redis_node_keys = nodes_dict = node_type = None
            node_key = ":".join([msg["ip"], msg["port"]])
            cluster_id = msg.get("cluster_id")
            node_info_data = {
                "scheduler_id": g_settings.server.scheduler_id,
                "cluster_id": cluster_id if cluster_id is not None else g_cluster_default_name,
                "ntype": msg["ntype"],
                "device_id": msg["device_id"],
                "ip": msg["ip"],
                "port": msg["port"],
                "update_time": current_time
            }

            if ntype == g_ntype_name_gateway:
                redis_prefix = g_redis_gwnode_prefix
                redis_node_keys = g_redis_gwnode_keys
                nodes_dict = g_node_gateway_dict
                node_type = g_redis_subscribe_gwnode_type

                node_info_data["api_port"] = msg["api_port"]
                node_info_data["vhosts"] = msg["vhosts"]
                node_info_data["nb_streams"] = msg["nb_streams"]
                # 控制心跳打印频率，首次/掉线后/流变化
                old = g_node_gateway_dict.get(node_key)
                if old is not None and old["nb_streams"] != node_info_data["nb_streams"]:
                    logger.info("heartbeat gateway:{} ip:{} port:{} change nb_streams:{} to {}".format(node_info_data["device_id"], node_info_data["ip"],
                                                                                                       node_info_data["port"], old["nb_streams"],
                                                                                                       node_info_data["nb_streams"]))

            elif ntype == g_ntype_name_record:
                redis_prefix = g_redis_rdnode_prefix
                redis_node_keys = g_redis_rdnode_keys
                nodes_dict = g_node_record_dict
                node_type = g_redis_subscribe_rdnode_type
                node_info_data["api_port"] = msg["api_port"]
                node_info_data["vhosts"] = msg["vhosts"]
                node_info_data["nb_streams"] = msg["nb_streams"]
                # 控制心跳打印频率，首次/掉线后/流变化
                old = g_node_record_dict.get(node_key)
                if old is not None and old["nb_streams"] != node_info_data["nb_streams"]:
                    logger.info("heartbeat record:{} ip:{} port:{} change nb_streams:{} to {}".format(node_info_data["device_id"], node_info_data["ip"],
                                                                                                      node_info_data["port"], old["nb_streams"],
                                                                                                      node_info_data["nb_streams"]))

            elif ntype == g_ntype_name_upload:
                redis_prefix = g_redis_upnode_prefix
                redis_node_keys = g_redis_upnode_keys
                nodes_dict = g_node_upload_dict
                node_type = g_redis_subscribe_upnode_type

            elif ntype == g_ntype_name_mprocess:
                redis_prefix = g_redis_mpnode_prefix
                redis_node_keys = g_redis_mpnode_keys
                nodes_dict = g_node_mprocess_dict
                node_type = g_redis_subscribe_mpnode_type
                node_info_data["process_count"] = msg["process_count"]
                node_info_data["upload_count"] = msg["upload_count"]

            await g_redis_client.set(redis_prefix + node_key, json.dumps(node_info_data), retry=True)
            # 如果拿到了新节点，通知其他调度加载新节点数据
            if node_key not in nodes_dict.keys():
                logger.info(f"heartbeat get new node:{node_info_data}")
                await g_redis_client.publish(g_redis_subscribe_channel, json.dumps({g_redis_subscribe_node_type: node_type, "data": node_info_data}))
                await g_redis_client.sadd(redis_node_keys, redis_prefix + node_key, retry=True)
            nodes_dict[node_key] = node_info_data

        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    logger.error("(MsgNotify) task_handle_async_api_heartbeat %d leave." % sever_index)


# 监听心跳状态变化，发现心跳异常的节点
async def task_heartbeat_monitor():
    try:
        # await asyncio.sleep(30)
        local_info = {}
        while True:
            lock_key = "scheduler_heartbeat_monitor_lock"
            lock_value = g_settings.server.scheduler_id  # 本机IP
            ret = await g_redis_client.lock(lock_key, lock_value, ex=15)
            if ret == 0:
                try:
                    now_time = time.time()
                    for redis_prefix, nodes in [(g_redis_gwnode_prefix, g_node_gateway_dict), (g_redis_rdnode_prefix, g_node_record_dict),
                                                (g_redis_upnode_prefix, g_node_upload_dict), (g_redis_mpnode_prefix, g_node_mprocess_dict)]:
                        for node in nodes.keys():
                            if now_time - nodes[node]["update_time"] > g_settings.server.heartbeat_interval + 1:
                                ret, value = await g_redis_client.get(redis_prefix + node, retry=True)
                                if ret == 0 and value is not None and len(value) > 0:
                                    msg = json.loads(value)
                                    if (now_time - msg["update_time"] > g_settings.server.heartbeat_interval + 1):
                                        logger.warning("{} {} ip:{} port:{} heartbeat error".format(nodes[node]["ntype"], nodes[node]["device_id"],
                                                                                                    nodes[node]["ip"], nodes[node]["port"]))
                                        # TODO 告警通知
                except Exception as e:
                    logger.error(f"get exception:{e} trace:" + traceback.format_exc())
                    # TODO 告警通知

                # script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
                # ret = await g_redis_client.eval(script, 1, lock_key, lock_value)
                ret = await g_redis_client.unlock(lock_key, lock_value)
                if ret != 0:
                    logger.error("scheduler heartbeat unlock error!")
            await asyncio.sleep(10)
    except Exception as e:
        logger.error(f"(MsgNotify) task_heartbeat_monitor get exception:{e} trace:" + traceback.format_exc())
        # TODO 通知异常


# 等待上传延时发布录制任务
async def task_wait_to_push():
    try:
        record_tasks = []
        while True:
            if len(g_wait_upload_tasks) > 0:
                now_time = int(time.time() * 1000)
                for mprocess_task_id, body in g_wait_upload_tasks.items():
                    stream_key = None
                    wait_upload = True
                    if len(body["domain"]) > 0:
                        stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
                    else:
                        stream_key = "_".join([g_default_domain_name, body["appName"], body["streamName"]])
                    ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
                    if ret == 0 and value is not None:
                        endTime = body.get("endTime")
                        record_stream_data = json.loads(value)
                        if record_stream_data["status"] == 1 and record_stream_data["Scount"] == record_stream_data["Ecount"]:
                            wait_upload = False
                        elif isinstance(endTime, int) and endTime <= record_stream_data["update_time"]:
                            wait_upload = False
                        elif now_time > record_stream_data["update_time"] + 120:
                            wait_upload = False
                            logger.warning("task update_time timeout record_stream_data:{} now:{}".format(record_stream_data, now_time))
                    else:
                        wait_upload = False

                    if wait_upload != True:
                        record_tasks.append(mprocess_task_id)
                        ret, llen = await g_redis_client.rpush(g_redis_mprocess_task_list, mprocess_task_id)
                        if ret == 0 and llen > 0:
                            logger.info(f"mprocess task create done id:{mprocess_task_id}")
                        else:
                            logger.warning(f"mprocess task push redis list error:{body}")
                            await g_redis_client.delete(mprocess_task_id)
                if len(record_tasks) > 0:
                    logger.info("record task send count:{}".format(len(record_tasks)))
                    for mprocess_task_id in record_tasks:
                        del g_wait_upload_tasks[mprocess_task_id]
                    record_tasks.clear()

            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())


def _subscribe_channel_handler(message):
    """
    接收redis订阅消息，如果有动态加载的节点，需要同步数据
    """
    try:
        subscribe_data = message['data']
        subscribe_msg = json.loads(subscribe_data)
        if subscribe_msg["data"]['scheduler_id'] == g_settings.server.scheduler_id:
            logger.debug("skip self publish msg")
            return

        logger.info(subscribe_data)
        if subscribe_msg[g_redis_subscribe_node_type] == g_redis_subscribe_gwnode_type or subscribe_msg[
                g_redis_subscribe_node_type] == g_redis_subscribe_rdnode_type:
            msg = copy.deepcopy(subscribe_msg["data"])

            node_key = ':'.join([msg['ip'], msg['port']])
            ntype = msg.get("ntype")
            if ntype == g_ntype_name_gateway:
                if node_key not in g_node_gateway_dict.keys():
                    logger.warning(f"find new node:{msg}")
                msg["scheduler_id"] = g_settings.server.scheduler_id
                g_node_gateway_dict[node_key] = msg
            elif ntype == g_ntype_name_record:
                if node_key not in g_node_record_dict.keys():
                    logger.warning(f"find new node:{msg}")
                msg["scheduler_id"] = g_settings.server.scheduler_id
                g_node_record_dict[node_key] = msg
            elif ntype == g_ntype_name_upload:
                if node_key not in g_node_upload_dict.keys():
                    logger.warning(f"find new node:{msg}")
                msg["scheduler_id"] = g_settings.server.scheduler_id
                g_node_upload_dict[node_key] = msg
            elif ntype == g_ntype_name_mprocess:
                if node_key not in g_node_mprocess_dict.keys():
                    logger.warning(f"find new node:{msg}")
                msg["scheduler_id"] = g_settings.server.scheduler_id
                g_node_mprocess_dict[node_key] = msg
            else:
                logger.warning("unknow ntype msg:{}".format(msg))
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())


async def task_subscribe_node_in_thread():
    while True:
        try:
            pubsub = PubSub(g_redis_client)
            await pubsub.subscribe(**{g_redis_subscribe_channel: _subscribe_channel_handler})

            thread = pubsub.run_in_thread(daemon=True)
            while True:
                await asyncio.sleep(10)
            pubsub.stop()
            await asyncio.sleep(10)
            logger.error("(MsgNotify) pubsub exit")
        except Exception as e:
            logger.error(f"(MsgNotify) pubsub exit get exception:{e} trace:" + traceback.format_exc())
            await asyncio.sleep(10)


async def task_handle_async_api(sever_index=0):
    """
    { 网关：释放录制节点
        "action": "ReleaseRecordNode",
        "domain":"",
        "vhost":"",
        "appName":"",
        "streamName":"",
        "cluster_id":"",
        "ip":"",
        "port":"",
    }
    { +++ 录制开始/录制结束
        "action": "RecordStreamStart / RecordStreamStop",
        "taskId": "", 
    }

    { 切片生成
        "action": "RecordStreamUpdateS",
        "domain":"",
        "vhost":"",
        "appName":"",
        "streamName":"",

        "taskId":"",
        "file":"",
    }
    {  正常推流 upload
        "action": "RecordStreamUpdateE",
        "domain": "", 
        "vhost": "", 
        "appName": "", 
        "streamName": "", 
        "taskId": "", 
        "tsPath": "", 
        "m3u8Path": "", 
        "streamStatus": "", 
    }
    {  reupload
        "action": "RecordStreamUpdateE",
        "domain": "", 
        "appName": "", 
        "streamName": "", 
    }
    """
    logger.info("task_handle_async_api start.")
    while True:
        try:
            now_time = int(time.time() * 1000)
            body = await g_msg_queue_trace_req.get()
            stream_key = None
            if len(body["domain"]) > 0:
                stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
            else:
                stream_key = "_".join([g_default_domain_name, body["appName"], body["streamName"]])

            # 网关节点，断流
            action = body.get("action")
            if action == "ReleaseRecordNode":
                ret, value = await g_redis_client.get(g_redis_gateway_prefix + stream_key, retry=True)
                if ret == 0:
                    logger.info("{}".format(body))
                    gateway_stream_data = json.loads(value)
                    gateway_stream_data["status"] = 1
                    gateway_stream_data["update_time"] = now_time
                    gateway_stream_data["scheduler_id"] = g_settings.server.scheduler_id
                    await g_redis_client.set(g_redis_gateway_prefix + stream_key, json.dumps(gateway_stream_data), ex=g_redis_key_expire, retry=True)
                else:
                    logger.warning("stream_key:{} publish error!".format(stream_key))

            # 录制节点，录制开始
            elif action == "RecordStreamStart":
                ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
                if ret == 0 and value is not None:
                    logger.info("Restart:{}".format(body))
                    # 再次推流 如果上次数据没有传完，可以做个新的记录
                    record_stream_data = json.loads(value)
                    record_stream_data["taskId"] = body.get("taskId")
                    record_stream_data["ip"] = body.get("ip")
                    record_stream_data["port"] = body.get("port")
                    record_stream_data["start_time"] = now_time
                    record_stream_data["scheduler_id"] = g_settings.server.scheduler_id
                    record_stream_data["status"] = 0
                    record_stream_data["Scount"] = 0
                    record_stream_data["Ecount"] = 0
                    await g_redis_client.set(g_redis_record_prefix + stream_key, json.dumps(record_stream_data), ex=g_redis_key_expire, retry=True)
                else:
                    logger.info("First start:{}".format(body))
                    # 首次创建
                    record_stream_data = {
                        "cluster_id": body["cluster_id"],
                        "domain": body["domain"],
                        "vhost": body["vhost"],
                        "appName": body["appName"],
                        "streamName": body["streamName"],
                        "taskId": body["taskId"],
                        "ip": body["ip"],
                        "port": body["port"],
                        "status": 0,
                        "Scount": 0,
                        "Ecount": 0,
                        "create_time": now_time,
                        "start_time": now_time,
                        "scheduler_id": g_settings.server.scheduler_id,
                    }
                    await g_redis_client.set(g_redis_record_prefix + stream_key, json.dumps(record_stream_data), ex=g_redis_key_expire, retry=True)

            # 录制节点，录制上传进度更新
            elif action == "RecordStreamUpdateS":
                ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
                if ret == 0 and value is not None:
                    record_stream_data = json.loads(value)
                    if record_stream_data["taskId"] == body.get("taskId"):
                        record_stream_data["update_time"] = now_time
                        record_stream_data["Scount"] = record_stream_data["Scount"] + 1
                        record_stream_data["scheduler_id"] = g_settings.server.scheduler_id
                        await g_redis_client.set(g_redis_record_prefix + stream_key, json.dumps(record_stream_data), ex=g_redis_key_expire, retry=True)
                        logger.info("action:{} ts:{} Scount:{} Ecount:{}".format(body.get("action"), body.get("file"), record_stream_data["Scount"], record_stream_data["Ecount"]))
                    else:
                        logger.warning("skip taskId:{} {}".format(body.get("taskId"), body))
                else:
                    logger.warning("unknow stream_key:{} {}".format(stream_key, body))

            # 录制节点，录制上传进度更新
            elif action == "RecordStreamUpdateE":
                ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
                if ret == 0 and value is not None:
                    record_stream_data = json.loads(value)
                    if record_stream_data["taskId"] == body.get("taskId"):
                        record_stream_data["update_time"] = now_time
                        record_stream_data["Ecount"] = record_stream_data["Ecount"] + 1
                        record_stream_data["scheduler_id"] = g_settings.server.scheduler_id
                        await g_redis_client.set(g_redis_record_prefix + stream_key, json.dumps(record_stream_data), ex=g_redis_key_expire, retry=True)
                        logger.info("action:{} ts:{} Scount:{} Ecount:{}".format(body.get("action"), body.get("tsPath"), record_stream_data["Scount"], record_stream_data["Ecount"]))
                    else:
                        logger.warning("skip taskId:{} {}".format(body.get("taskId"), body))
                else:
                    logger.warning("unknow stream_key:{} {}".format(stream_key, body))

            # 录制节点，结束录制
            elif action == "RecordStreamStop":
                ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
                if ret == 0 and value is not None:
                    record_stream_data = json.loads(value)
                    record_stream_data["status"] = 1
                    record_stream_data["stop_time"] = now_time
                    record_stream_data["scheduler_id"] = g_settings.server.scheduler_id
                    await g_redis_client.set(g_redis_record_prefix + stream_key, json.dumps(record_stream_data), ex=g_redis_key_expire, retry=True)
                    logger.info("info:{} Scount:{} Ecount:{}".format(body, record_stream_data["Scount"], record_stream_data["Ecount"]))
                    # TODO 推流结束后，如果切片计数差距较大，需要告警
                else:
                    logger.warning("unknow stream_key:{} {}".format(stream_key, body))
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    logger.error("(MsgNotify) task_handle_async_api %d leave." % sever_index)


async def task_handle_async_mprocess_cbk():
    """
    { 
        "action" : "UpdateMediaProcessTask",
        "mprocess_task_id" : "...",
    }
    """
    logger.info("task_handle_async_mprocess_cbk start.")

    callback_url = None
    if g_settings.get('manager') is not None and g_settings.manager.get('callback') is not None:
        callback_url = g_settings.manager.callback
    while True:
        try:
            body = await g_msg_queue_mprocess_cbk.get()
            logger.info("mprocess_cbk info:{}".format(body))

            mprocess_task_id = body.get("mprocess_task_id")
            if mprocess_task_id is not None:
                ret, value = await g_redis_client.get(mprocess_task_id, json.dumps(body))
                if ret == 0 and len(value) > 0:
                    info = json.loads(value)
                    logger.info(f"mprocess done task:{info}")

                    # send callback to manager center
                    url = info.get("callback")
                    if url is None or len(url) <= 0 or url.lower().find('http') == -1:
                        url = callback_url
                    if url is not None and len(url) > 0 and url.lower().find('http') != -1:
                        # TODO. 组织必要的回调参数，返回给管理中心
                        cbk_info = {
                            "mprocess_task_id": info.get("mprocess_task_id"),
                            "domain": info.get("domain"),
                            "appName": info.get("appName"),
                            "streamName": info.get("streamName"),
                            "startTime": info.get("startTime"),
                            "endTime": info.get("endTime"),
                            "bucketName": info.get("bucketName"),
                            "objectKey": info.get("objectKey"),
                            "media_info": info.get("media_info"),
                            "status": info.get("mprocess_status"),
                        }
                        status, text = await send_request_json(url, json_data=cbk_info)
                        if status != 200:
                            logger.warning(f"mprocess done callback error:{status} {text}")
                        else:
                            await g_redis_client.delete(mprocess_task_id)  # 回调以后立即删除任务信息
                    else:
                        logger.warning("mprocess done no callback")
                else:
                    logger.warning("get info error(redis)")
            else:
                logger.warning(f"mprocess_task_id is None body:{body}")
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    logger.error("(MsgNotify) task_handle_async_mprocess_cbk leave")


async def _alloc_record_node_check_valid(now_time, body, node_key):
    try:
        ret = False
        nodeinfo = None

        cluster_id = body.get("cluster_id")
        if cluster_id is None or cluster_id == "":
            cluster_id = g_cluster_default_name
        nodeinfo = g_node_record_dict.get(node_key)
        if nodeinfo is None:
            return ret, nodeinfo
        # vhost 使用动态生成方式，无需特定匹配, body["vhost"] not in nodeinfo["vhosts"] or
        if (cluster_id is not None and cluster_id != nodeinfo.get("cluster_id")):
            return ret, nodeinfo
        # 获节点信息
        node_key = g_redis_rdnode_prefix + "{}:{}".format(nodeinfo["ip"], nodeinfo["port"])
        status, value = await g_redis_client.get(node_key, retry=True)
        if status == -1:
            # TODO 告警通知
            logger.warning("find online record nodeinfo:{} error!".format(nodeinfo))
            # 认为redis已经异常了，但是在一定范围内，也要尽可能的保证数据能录制
            if nodeinfo["nb_streams"] < g_settings.server.max_record_nbstreams * 0.6:
                nodeinfo["nb_streams"] += 1
            else:
                logger.warning("node:{} nstreams max:{} cur:{}".format(node_key, g_settings.server.max_record_nbstreams, nodeinfo["nb_streams"]))
                return ret, nodeinfo
        else:
            nodeinfo = json.loads(value)
            # 心跳异常，任务服务已经异常
            if (now_time - nodeinfo["update_time"] > 2 * g_settings.server.heartbeat_interval):
                logger.warning("node:{} heartbeat timeout:{} skip.".format(node_key, now_time - nodeinfo["update_time"]))
                return ret, nodeinfo
            # 超过负载
            if nodeinfo["nb_streams"] >= g_settings.server.max_record_nbstreams:
                logger.warning("node:{} nstreams max:{} cur:{}".format(node_key, g_settings.server.max_record_nbstreams, nodeinfo["nb_streams"]))
                return ret, nodeinfo
        ret = True
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    finally:
        return ret, nodeinfo


async def _alloc_record_node_check_history(now_time, body):
    try:
        ret = False
        nodeinfo = None
        stream_key = None

        if len(body["domain"]) > 0:
            stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
        else:
            stream_key = "_".join([g_default_domain_name, body["appName"], body["streamName"]])
        cluster_id = body.get("cluster_id")
        if cluster_id is None or cluster_id == "":
            cluster_id = g_cluster_default_name

        # 优先分配历史节点
        flag, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
        if flag == 0 and value is not None and len(value) > 0:
            record_stream_data = json.loads(value)
            node_key = ':'.join([record_stream_data["ip"], record_stream_data["port"]])
            ret, nodeinfo = await _alloc_record_node_check_valid(now_time, body, node_key)
            if True == ret:
                logger.info("use history node")
            else:
                # 历史节点超过负载，再选择同机器的节点
                for node_key in list(g_node_record_dict.keys()):
                    if node_key.startswith(record_stream_data["ip"]):
                        ret, nodeinfo = await _alloc_record_node_check_valid(now_time, body, node_key)
                        if True == ret:
                            logger.info("use history node same ip machine node")
                            break
        # 认为redis异常了，此时也要带上恢复标识，返回一个非空值，保证数据正确录制
        elif flag == -1:
            nodeinfo = {}
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    finally:
        return ret, nodeinfo


async def _handle_alloc_record_node(body):
    """
    {
        "action": "AllocRecordNode",
        "domain":"",
        "vhost":"",
        "appName":"",
        "streamName":"",
        "cluster_id":"",
        "ip":"",
        "port":"",
    }
    """
    response_info = {"status": 1, "ip": "", "port": ""}
    record_node_found = False
    stream_key = None

    try:
        logger.info("body:{}".format(body))
        now_time = time.time()
        if len(body["domain"]) > 0:
            stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
        else:
            stream_key = "_".join([g_default_domain_name, body["appName"], body["streamName"]])
        cluster_id = body.get("cluster_id")
        if cluster_id is None or cluster_id == "":
            cluster_id = g_cluster_default_name

        # 优先分配历史节点
        recover_flag = False
        node_key = None
        ret, nodeinfo = await _alloc_record_node_check_history(now_time, body)
        if True != ret:
            # 有历史节点，但是由于负载等限制条件，节点不可用
            if nodeinfo is not None:
                recover_flag = True

            # 首次全新的流/重新 随机分配节点
            node_keys = list(g_node_record_dict.keys())
            random.shuffle(node_keys)
            for node_key in node_keys:
                ret, nodeinfo = await _alloc_record_node_check_valid(now_time, body, node_key)
                if True != ret:
                    continue
                else:
                    logger.info("use new node")
                    break
        # 找到可用历史节点，带上恢复标识
        else:
            recover_flag = True

        if True == ret and nodeinfo is not None:
            if node_key is None:
                node_key = ":".join([nodeinfo["ip"], nodeinfo["port"]])
            # 存储Node节点信息
            # 数量+1，这样做有可能大并发的时候数量不对，要通过心跳来纠正
            nodeinfo["nb_streams"] += 1
            # nodeinfo["update_time"] = now_time 此处不能更新时间，会导致心跳误判
            nodeinfo["scheduler_id"] = g_settings.server.scheduler_id
            await g_redis_client.set(g_redis_rdnode_prefix + node_key, json.dumps(nodeinfo), retry=True)

            # 更新gateway节点-流信息
            gateway_stream_data = {
                "cluster_id": cluster_id,
                "domain": body["domain"],
                "vhost": body["vhost"],
                "appName": body["appName"],
                "streamName": body["streamName"],
                "ip": body["ip"],
                "port": body["port"],
                "status": 0,
                "update_time": now_time,
                "scheduler_id": g_settings.server.scheduler_id,
            }
            await g_redis_client.set(g_redis_gateway_prefix + stream_key, json.dumps(gateway_stream_data), ex=g_redis_key_expire, retry=True)

            # 调度结果
            response_info["status"] = 0
            response_info["ip"] = nodeinfo["ip"]
            response_info["port"] = nodeinfo["port"]
            if recover_flag:
                response_info["recover_flag"] = "true"
            record_node_found = True

            # 节点信息变化，需要通知同步数据更新
            # if sync_flag:
            #     response_info["sync_flag"] = 1
            #     await g_redis_client.publish(g_redis_subscribe_channel, json.dumps({"type":g_redis_subscribe_pstream, "data":gateway_stream_data}))
            #     await g_redis_client.publish(g_redis_subscribe_channel, json.dumps({"type":g_redis_subscribe_rstream, "data":record_stream_data}))
        else:
            logger.error("!!! stream_key:{} no node to record !!!".format(stream_key))
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    finally:
        if not record_node_found:
            logger.error("record node not found: {}".format(stream_key))
        logger.info("stream_key:{} response_info: {}".format(stream_key, response_info))
        return response_info


async def _handle_api_service(request):
    try:
        body = await request.json()
        logger.debug("Request body:{}".format(body))
        action = body.get("action")
        # 网关节点，分配录制节点
        if action == "AllocRecordNode":
            response_info = await _handle_alloc_record_node(body)
            return web.Response(text=json.dumps(response_info), content_type='application/json')
        # 网关节点，释放录制节点
        elif action == "ReleaseRecordNode":
            await g_msg_queue_trace_req.put(body)
            status_info = {"status": 0}
            return web.Response(text=json.dumps(status_info), content_type='application/json')

        # 录制节点，开始录制
        elif action == "RecordStreamStart":
            await g_msg_queue_trace_req.put(body)
            status_info = {"status": 0}
            return web.Response(text=json.dumps(status_info), content_type='application/json')
        # 录制节点，录制进度更新 切片生成
        elif action == "RecordStreamUpdateS":
            await g_msg_queue_trace_req.put(body)
            status_info = {"status": 0}
            return web.Response(text=json.dumps(status_info), content_type='application/json')
        # 录制节点，录制进度更新 切片存储完毕
        elif action == "RecordStreamUpdateE":
            await g_msg_queue_trace_req.put(body)
            status_info = {"status": 0}
            return web.Response(text=json.dumps(status_info), content_type='application/json')
        # 录制节点，结束录制
        elif action == "RecordStreamStop":
            await g_msg_queue_trace_req.put(body)
            status_info = {"status": 0}
            return web.Response(text=json.dumps(status_info), content_type='application/json')

        # 心跳
        elif action == "Heartbeat":
            await g_msg_queue_heartbeat.put(body)
            return web.Response(text="0")
        else:
            logger.warning("Unknow Request body:{}".format(body))
            return web.Response(text="-1")
    except Exception as e:
        logger.error("request:{}".format(request))
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return web.Response(text="-1")


async def _handle_mprocess_service(request):
    """
    {
        "action": "CreateMediaProcessTask",
        "startTime": 1611212411659 + 15000,
        "endTime": 1611212753626,
        "domain": "www.test.com",
        "appName": "live",
        "streamName": "ok",
        "objectKey": "",
        "target": "IOBS-IOT",
        "host": "stg-iobs-upload.pingan.com.cn",
        "token":""
        "tokenUpdate":"http://...",
    }    
    {
        "action": "CreateMediaProcessTask",
        "startTime": 1611212411659 + 15000,
        "endTime": 1611227815087,
        "domain": "www.test.com",
        "appName": "live",
        "streamName": "ok",
        "objectKey": "",
        "target": "OBS/IOBS",
        "host": "obs-cn-shenzhen.yun.pingan.com",
        "bucketName": "paavc-vod-publictest4",
        "accessKey": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
        "secretKey": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
    }
    """
    try:
        body = await request.json()
        logger.info(f"mprocess task:{body}")
        action = body.get("action")
        # 创建录制任务，加入任务队列
        if action == "CreateMediaProcessTask":
            # 1、接收录制任务
            # 2、检查录制任务是否就绪(如果全量录制，需要适当等待切片上传完毕)
            # 3、发送到 redis list中
            # 4、同时保存redis字典中   状态：已发送、未发送
            # 5、mprocess 处理完以后，回调任务结果
            # 6、本地任务管理器更新状态，返回给管理中心
            # 7、提供查询接口：排队中、执行中、执行完毕(成功/失败/超时)
            stream_key = None
            now_time = int(time.time() * 1000)
            wait_upload = True
            if len(body["domain"]) > 0:
                stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
            else:
                stream_key = "_".join([g_default_domain_name, body["appName"], body["streamName"]])
            ret, value = await g_redis_client.get(g_redis_record_prefix + stream_key, retry=True)
            if ret == 0 and value is not None:
                endTime = body.get("endTime")
                record_stream_data = json.loads(value)
                if record_stream_data["status"] == 1 and record_stream_data["Scount"] == record_stream_data["Ecount"]:
                    wait_upload = False
                elif isinstance(endTime, int) and endTime <= record_stream_data["update_time"]:
                    wait_upload = False
                elif now_time > record_stream_data["update_time"] + 300:
                    wait_upload = False
                    logger.warning("record_stream_data:{} now:{}".format(record_stream_data, now_time))
            else:
                wait_upload = False

            random_value = random.randint(0, 1000000)
            mprocess_task_id = str(hashlib.md5((str(now_time) + str(random_value) + json.dumps(body)).encode(encoding='UTF-8')).hexdigest())
            body["mprocess_task_id"] = mprocess_task_id
            body["mprocess_status"] = 0
            ret = await g_redis_client.set(mprocess_task_id, json.dumps(body), ex=604800)  # 任务信息保存七天
            if ret == 0:
                if wait_upload != True:
                    ret, llen = await g_redis_client.rpush(g_redis_mprocess_task_list, mprocess_task_id)
                    if ret == 0 and llen > 0:
                        logger.info(f"mprocess task create done id:{mprocess_task_id}")
                        response_info = {"code": 20000, "msg": "OK", "mprocess_task_id": mprocess_task_id}
                        return web.Response(text=json.dumps(response_info), content_type='application/json')
                    else:
                        logger.warning(f"mprocess task push redis list error:{body}")
                        await g_redis_client.delete(mprocess_task_id)
                        response_info = {"code": 40000, "msg": "Task List Push Error(redis)"}
                        return web.Response(text=json.dumps(response_info), content_type='application/json')
                else:
                    logger.info(f"mprocess task create id:{mprocess_task_id} wait for upload done")
                    g_wait_upload_tasks[mprocess_task_id] = body
                    response_info = {"code": 20000, "msg": "OK", "mprocess_task_id": mprocess_task_id}
                    return web.Response(text=json.dumps(response_info), content_type='application/json')
            else:
                logger.warning("mprocess task add redis error")
                # TODO. 随机挑选一个节点REST方式发送请求
                response_info = {"code": 40000, "msg": "Task Create Error(redis)"}
                return web.Response(text=json.dumps(response_info), content_type='application/json')
        # 查询录制任务(已完成的任务不保存，只返回排队中/执行中状态)
        elif action == "CheckMediaProcessTask":
            mprocess_task_id = body.get("mprocess_task_id")
            if mprocess_task_id is not None:
                ret, value = await g_redis_client.get(mprocess_task_id, json.dumps(body))
                if ret == 0 and len(value) > 0:
                    info = json.loads(value)
                    response_info = {"code": 20000, "msg": "OK", "mprocess_task_id": mprocess_task_id, "mprocess_status": info["mprocess_status"]}
                    return web.Response(text=json.dumps(response_info), content_type='application/json')
            response_info = {"code": 40000, "msg": "Task ID Error"}
            return web.Response(text=json.dumps(response_info), content_type='application/json')
        # 录制录制任务完成
        elif action == "UpdateMediaProcessTask":
            await g_msg_queue_mprocess_cbk.put(body)
            return web.Response(text="0")
        else:
            logger.warning("Unknow Request body:{}".format(body))
            response_info = {"code": 40000, "msg": "Unknow Request action"}
            return web.Response(text=json.dumps(response_info), content_type='application/json')
    except Exception as e:
        logger.error("request:{}".format(request))
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        response_info = {"code": 40000, "msg": "Internal Error"}
        return web.Response(text=json.dumps(response_info), content_type='application/json')


async def task_scheduler_server_api():
    """
    启动调度服务
    """
    # app = web.Application(loop=g_event_loop, middlewares=[_api_logger_factory])
    app = web.Application()
    app.add_routes([web.post(g_settings.server.scheduler_api_path, _handle_api_service)])
    app.add_routes([web.post(g_settings.server.scheduler_mprocess_path, _handle_mprocess_service)])
    app_runner = web.AppRunner(app)
    await app_runner.setup()
    srv = await g_event_loop.create_server(
        app_runner.server,
        g_settings.server.scheduler_ip,
        g_settings.server.scheduler_port,
    )
    logger.info("task_scheduler_server_api started!")
    return srv


def _signal_reload(signalNumber, frame):
    try:
        global g_settings
        reload(scheduler_settings)
        g_settings = dict_to_Dict(scheduler_settings.configs)
        logger.info("settings reload:{}".format(g_settings))
    except Exception as e:
        logger.error(f"settings reload get exception:{e} trace:" + traceback.format_exc())


if __name__ == "__main__":
    try:
        logger.info('record_scheduler_server starting!')
        pes_update_config()
        signal.signal(signal.SIGHUP, _signal_reload)

        # 连接redis
        g_redis_client = RedisClient(g_settings.redis.host, g_settings.redis.port, g_settings.redis.password, g_settings.redis.cluster)
        g_redis_client.connect_to_redis()

        tasks = []

        # 定时加载Redis数据
        tasks.append(task_load_info_from_redis())
        # 订阅多机调度数据同步
        tasks.append(task_subscribe_node_in_thread())

        # 启动调度REST API服务
        tasks.append(task_scheduler_server_api())
        # API-节点处理
        tasks.append(task_handle_async_api())
        # API-节点心跳监测
        tasks.append(task_handle_async_api_heartbeat())
        # API-后处理任务回调
        tasks.append(task_handle_async_mprocess_cbk())
        # API-后处理任务延时发送
        tasks.append(task_wait_to_push())

        # 监听服务节点心跳状态
        tasks.append(task_heartbeat_monitor())

        try:
            g_event_loop.run_until_complete(asyncio.wait(tasks))
            g_event_loop.close()
        except KeyboardInterrupt:
            logger.critical("<Got Signal: SIGINT, shutting down.>")
    except Exception as e:
        logger.error(f"(MsgNotify) get exception:{e} trace:" + traceback.format_exc())

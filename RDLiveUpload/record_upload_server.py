#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import os
import time
import re
import random
import shutil
import traceback
import asyncio
import signal
import socket
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

import copy
import aiohttp
from aiohttp import web
from aiohttp import ClientConnectionError
from obs import ObsOperator, ObjectMetadata, MultipartUploadFileRequest
from obs.util import *
from obs.exception import AmazonClientException
from concurrent.futures import ThreadPoolExecutor
from record_obs_hash import BucketHash
from importlib import reload
import record_upload_config as upload_settings
from record_common import pes_get_configmap_str, pes_get_configmap_int, send_request_json, send_request_params, dict_to_Dict
from urllib.error import HTTPError

g_settings = dict_to_Dict(upload_settings.configs)  # 服务启动配置
logging.basicConfig(
    level=logging.WARNING,
    format='[%(asctime)s-%(filename)s-%(lineno)d]:%(levelname)s: %(message)s',  # -%(funcName)s
    handlers=[logging.StreamHandler(),
              TimedRotatingFileHandler(filename=g_settings.server.log_file, when="D", interval=1, backupCount=30)]  # S M H D MIDNIGHT
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

g_default_domain_name = "push.localhost"
g_msg_queue_upload = asyncio.Queue()
g_msg_queue_reupload = asyncio.Queue()
g_event_loop = asyncio.get_event_loop()  # 异步句柄
g_upload_segment_task = {}  # 记录上传切片任务
g_upload_stream_task = {}  # 记录流状态
g_reupload_files = {}  # 记录重传文件次数
g_obs_info = {}  # 记录OBS目标
g_obs_bucket_hash = BucketHash()


def pes_update_config():
    pass
    # value = pes_get_configmap_str("/tmp/upload/temp_dir")
    # if value is not None:
    #     g_settings.server.temp_dir = value

    # value = pes_get_configmap_int("/tmp/upload/temp_dir_del_MB")
    # if value is not None:
    #     g_settings.server.temp_dir_del_MB = value


class UploadStatistics:
    def __init__(self):
        super().__init__()
        self.taskId = None
        self.domain = None
        self.vhost = None
        self.appName = None
        self.streamName = None

        self.start_utc = 0  # 统计开始时间
        self.update_utc = 0  # 最近更新时间
        self.segments = {}  # 记录所有上传的切片标记
        self.end_utc = 0  # 流是否结束, 0默认值，!=0 结束
        self.uploading_count = 0  # 正在传输切片计数
        self.uploaded_count = 0  # 传输完毕切片计数


async def task_heartbeat():
    """
    # 心跳上报
    # 当前上传中流数量
    # 当前上传的速度
    {
        "action" : "Heartbeat"
        通用信息
        "cluster_id" : ""
        "ntype" : "rd_upload"
        "device_id" : ""
        "ip" : ""
        "port" : ""

        特定节点信息
    }
    """
    ipaddr = socket.gethostbyname(socket.gethostname())
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    # TODO. 统计数据
                    # task_info_list = sorted(g_upload_segment_task.items(), key=lambda kv: (kv[1].start_utc), reverse=True)
                    # for task_info_item in task_info_list:
                    #     taskId = task_info_item[0]
                    #     task_info = task_info_item[1]
                    #     if task_info.end_utc > 0:
                    json_data = {
                        "action": "Heartbeat",
                        "cluster_id": g_settings.server.cluster_id,
                        "ntype": "rd_upload",
                        "device_id": g_settings.server.device_id,
                        "ip": ipaddr,
                        "port": str(g_settings.server.service_port)
                    }
                    async with session.post(g_settings.scheduler.api, json=json_data, timeout=5) as resp:
                        # logger.info("Response status: {} data: {} taskId: {} ".format(status, text, task_id))
                        await asyncio.sleep(10)
        except ClientConnectionError:
            logger.error('heartbeat connect to host error')
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        finally:
            await asyncio.sleep(5)


def _check_obs_info(stream_key):
    try:
        return {
            "host": "obs-cn-shenzhen-internal.cloud.papub",  # 100.68.99.100
            "bucket": g_obs_bucket_hash.getBucket(stream_key),
            "access_key": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
            "secret_key": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
        }
    except KeyError as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return None


async def _api_logger_factory(app, handler):
    async def log(request):
        # logger.info('Request: %s %s' % (request.method, request.path))
        return await handler(request)

    return log


async def _handle_obs_upload(request):
    """
    接收上传任务，存储上传队列、统计上传任务数据
    {
        "taskId": "", 
        "domain": "", 
        "vhost": "", 
        "appName": "", 
        "streamName": "", 
        "tsPath": "", 
        "m3u8Path": "", 
        "streamStatus": "", 
    }
    """
    try:
        body = await request.json()
        logger.debug('in {}'.format(body))

        taskId = body.get("taskId")
        domain = body.get("domain")
        vhost = body.get("vhost")
        appName = body.get("appName")
        streamName = body.get("streamName")
        tsPath = body.get("tsPath")
        m3u8Path = body.get("m3u8Path")
        if domain is None or len(domain) <= 0:
            domain = g_default_domain_name
            body["domain"] = g_default_domain_name
        if taskId is None:
            taskId = "default_taskId_" + vhost + appName + streamName
            body["taskId"] = taskId
        stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
        # obs_info = _check_obs_info(stream_key)

        # 统计上传信息
        task_info = g_upload_segment_task.get(taskId)
        if task_info is None:
            task_info = UploadStatistics()
            task_info.taskId = taskId
            task_info.domain = domain
            task_info.vhost = vhost
            task_info.appName = appName
            task_info.streamName = streamName
            task_info.start_utc = task_info.update_utc = int(time.time())
            g_upload_segment_task[taskId] = task_info

        stream_info = g_upload_stream_task.get(stream_key)
        if stream_info is None:
            stream_info = list()
            stream_info.append(task_info)
            g_upload_stream_task[stream_key] = stream_info

            # 如果有本地磁盘，创建目录备用缓存
            local_backup_dir = g_settings.server.get("local_backup_dir")
            if local_backup_dir is not None:
                os.makedirs("/".join([local_backup_dir, domain, appName, streamName]), exist_ok=True)

            # 如果有本地磁盘，创建目录备用缓存
            local_store_dir = g_settings.server.get("local_store_dir")
            if local_store_dir is not None:
                os.makedirs("/".join([local_store_dir, domain, appName, streamName]), exist_ok=True)

        # 将上传文件任务添加到 queue 中，等待 task 消费
        if tsPath is not None and len(tsPath) > 0:
            seg_key = taskId + tsPath.split("/")[-1]
            # stat = os.stat(tsPath)
            task_info.segments[seg_key] = {"filePath": tsPath, "m3u8Path": m3u8Path, "utc": time.time()}  #, "size": stat.st_size
            task_info.uploading_count += 1
            task_info.update_utc = int(time.time())
        # 推流结束
        if body.get("streamStatus") == 1:
            task_info = g_upload_segment_task[taskId]
            task_info.end_utc = int(time.time())
            logger.info('upload bucket info:{}'.format(_check_obs_info(stream_key)))

        await g_msg_queue_upload.put(body)
        logger.info('recv upload ts:{}'.format(body.get("tsPath")))
        return web.Response(text="0")
    except KeyError as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return web.Response(text="-1")


async def _handle_obs_download(request):
    """
    接收上传任务，存储上传队列、统计上传任务数据
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "path": "", 
    }
    """
    try:
        body = await request.json()
        result = await g_event_loop.run_in_executor(None, _download_worker, body)
        if result is not None:
            return web.Response(text="0")
        else:
            logger.warning("download {} failed".format(body))
            return web.Response(text="-1")
    except KeyError as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return web.Response(text="-1")

async def _handle_obs_m3u8_recover(request):
    """
    接收上传任务，存储上传队列、统计上传任务数据
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "path": "", 
    }
    """
    try:
        body = await request.json()
        # 如果文件存在，下载线上文件，较大的文件认为是最新的
        if os.path.exists(body["path"]):
            recover_body = copy.deepcopy(body)
            recover_body["postfix"] = ".tmp"
            result = await g_event_loop.run_in_executor(None, _download_worker, recover_body)
            if result is not None:
                local_stat = os.stat(body["path"])
                online_stat = os.stat(recover_body["path"] + recover_body["postfix"])
                if online_stat.st_size > local_stat.st_size:
                    os.remove(body["path"])
                    os.rename(recover_body["path"] + recover_body["postfix"], body["path"])
                    logger.info("recover done use online file {}".format(body))
                else:
                    os.remove(recover_body["path"] + recover_body["postfix"])
                    logger.info("recover done use local file {}".format(body))
                return web.Response(text="0")
            else:
                logger.warning("download {} failed".format(body))
                return web.Response(text="-1")
        # 直接下载
        else:
            result = await g_event_loop.run_in_executor(None, _download_worker, body)
            if result is not None:
                logger.info("recover done {}".format(body))
                return web.Response(text="0")
            else:
                logger.warning("download {} failed".format(body))
                return web.Response(text="-1")
    except KeyError as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return web.Response(text="-1")


async def task_record_upload_server():
    try:
        logger.info('started!')
        # app = web.Application(loop=g_event_loop, middlewares=[_api_logger_factory])
        app = web.Application()
        app.add_routes([web.post(g_settings.server.obs_upload, _handle_obs_upload)])
        app.add_routes([web.post(g_settings.server.obs_download, _handle_obs_download)])
        app.add_routes([web.post(g_settings.server.obs_recover, _handle_obs_m3u8_recover)])
        app_runner = web.AppRunner(app)
        await app_runner.setup()
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
    srv = await g_event_loop.create_server(app_runner.server, g_settings.server.service_ip, g_settings.server.service_port)
    return srv


def _upload_worker(body, path):
    """
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "path": "", 
    }
    """
    try:
        if os.path.exists(path):
            stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
            object_key = "|".join([g_settings.server.get("cluster_id"), body["domain"], body["appName"], body["streamName"], path.split("/")[-1]])
            obs_info = _check_obs_info(stream_key)
            obs = ObsOperator(obs_info["host"], obs_info["access_key"], obs_info["secret_key"])
            ret = obs.put_object_from_file(obs_info["bucket"], object_key, path)
            return ret
        else:
            return None
    except Exception as e:
        logger.error(f"body:{body} path:{path}")
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return None


def _download_worker(body):
    """
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "path": "", 
        "postfix":""
    }
    """
    try:
        domain = body.get("domain")
        if domain is None or len(domain) <= 0:
            domain = g_default_domain_name
            body["domain"] = g_default_domain_name
        stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
        object_key = "|".join([g_settings.server.get("cluster_id"), body["domain"], body["appName"], body["streamName"], body["path"].split("/")[-1]])
        obs_info = _check_obs_info(stream_key)
        obs = ObsOperator(obs_info["host"], obs_info["access_key"], obs_info["secret_key"])
        s3_object = obs.get_object(obs_info["bucket"], object_key)
        path = body.get("path")
        postfix = body.get("postfix")
        if postfix is not None and len(postfix) > 0:
            path = path + str(postfix)
        with open(path, "wb") as to_file:
            s3_object.to_file(to_file)
        logger.info("download done {}".format(body))
        return body
    except Exception as e:
        if isinstance(e, AmazonClientException):   # isinstance(e, HTTPError) and e.code == 404
            logger.warning("recover get an exception:{} {} {}".format(e, obs_info, body))
            return None
        else:
            logger.error("get an exception:{} {} {}".format(e, obs_info, body))
            logger.error("exception trace: " + traceback.format_exc())
            return None


async def task_record_upload_runner(index):
    """
    实时录制上传TS 及 M3U8 文件
    {
        "taskId": "", 
        "domain": "", 
        "vhost": "", 
        "appName": "", 
        "streamName": "", 
        "tsPath": "", 
        "m3u8Path": "", 
        "streamStatus": "", 
    }
    """
    logger.info(f'i:{index} task_record_upload_runner starting!')
    body = dict()
    # loop = asyncio.get_running_loop()  # 获取当前事件循环
    local_backup_dir = g_settings.server.get("local_backup_dir")
    local_store_dir = g_settings.server.get("local_store_dir")
    while True:
        try:
            body = await g_msg_queue_upload.get()
            # logger.info(f'i:{index} start upload body:{body}')
            logger.info("i:{} start upload ts:{} status:{}".format(index, body.get("tsPath"), body.get("streamStatus")))
            lost_flag = False
            domain = body.get("domain")
            if domain is None or len(domain) <= 0:
                domain = g_default_domain_name
                body["domain"] = g_default_domain_name
            appName = body.get("appName")
            streamName = body.get("streamName")

            # 如果有加速设置，直接执行数据拷贝(未启用)
            if local_store_dir is not None and body.get("fast") is not None:
                try:
                    shutil.copy(body.get("tsPath"), "/".join([local_store_dir, domain, appName, streamName]))
                    shutil.copy(body.get("m3u8Path"), "/".join([local_store_dir, domain, appName, streamName]))
                except Exception as e:
                    logger.error("local_store_dir shutil.copy get an exception:{}".format(e))

            task_info = g_upload_segment_task.get(body.get("taskId"))
            task_info.uploaded_count += 1
            result = await g_event_loop.run_in_executor(None, _upload_worker, body, body.get("tsPath"))
            if result is not None and result.get_e_tag() is not None:
                os.remove(body.get("tsPath"))
            else:
                lost_flag = True
                logger.warning("upload ts failed {}".format(body))
                # 数据移动到本地磁盘，后续再不断重试
                if local_backup_dir is not None:
                    shutil.move(body.get("tsPath"), "/".join([local_backup_dir, domain, appName, streamName]))

            result = await g_event_loop.run_in_executor(None, _upload_worker, body, body.get("m3u8Path"))
            if result is not None and result.get_e_tag() is not None:
                pass
            else:
                logger.warning("upload m3u8 failed {}".format(body))
                if local_backup_dir is not None:
                    shutil.copy(body.get("m3u8Path"), "/".join([local_backup_dir, domain, appName, streamName]))

        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        finally:
            # TODO. 优化为长链接方式更新
            body["action"] = "RecordStreamUpdateE"
            if lost_flag:
                body["lost"] = 1
            await send_request_json(g_settings.scheduler.api, body)
            logger.info("i:{} upload done ts:{}".format(index, body.get("tsPath")))


async def task_record_reupload_runner(index):
    """
    上传失败，重试上传任务
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "path": "",
    }
    """
    # loop = asyncio.get_running_loop()  # 获取当前事件循环
    local_backup_reupload_count = g_settings.server.get("local_backup_reupload_count")
    if local_backup_reupload_count is None:
        local_backup_reupload_count = 0
    while True:
        try:
            # 监控轮询同一个文件可能进入多次，处理掉后直接跳过
            body = await g_msg_queue_reupload.get()
            if os.path.exists(body.get("path")):
                reupload = g_reupload_files.get(body["path"])

                if reupload is None or reupload < local_backup_reupload_count:
                    result = await g_event_loop.run_in_executor(None, _upload_worker, body, body.get("path"))
                    if result is not None and result.get_e_tag() is not None:
                        logger.info("i:{} reupload ts {} done".format(index, body))
                        os.remove(body.get("path"))

                        body["action"] = "RecordStreamUpdateE"
                        await send_request_json(g_settings.scheduler.api, body)
                    else:
                        if reupload is None:
                            g_reupload_files[body["path"]] = 1
                            logger.warning("i:{} reupload 1 ts failed {}".format(index, body))
                        elif reupload < local_backup_reupload_count:
                            logger.warning("i:{} reupload {} ts failed {}".format(index, reupload, body))
                            g_reupload_files[body["path"]] = reupload + 1
                        # else:
                        #     logger.warning("i:{} reupload {} ts failed remove {}".format(index, reupload, body))
                        #     os.remove(body.get("path"))
                        #     del g_reupload_files[body.get("path")]
                else:
                    # 重传多次失败，数据不能删除，跳过暂时缓存本地
                    pass
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())


async def task_dirs_monitor(path=None, resend=False, mem=False, timeout=-1, default_timeouts=-1, ts_timeouts=-1, m3u8_timeouts=-1):
    """
    1、监控内存目录及目录下所有文件，超时后，移动到本地磁盘，记录再resend_file_list中，等待做resend处理
    (控制内存空间的占用，极端状态下，即使无法上传也不影响正常直播转码)
    """
    logger.info("path:{}".format(path))
    local_backup_dir = g_settings.server.get("local_backup_dir")
    while path is not None:
        try:
            flag = 0
            size = 0
            file_count = 0
            dir_count = 0
            empty_dir = 0

            unit = 100
            file_list = []
            empty_dir_list = []
            for root, dirs, files in os.walk(path):
                for name in files:
                    flag += 1
                    path_file = os.path.join(root, name)
                    stat = os.stat(path_file)
                    size += stat.st_size
                    file_count += 1
                    file_list.append({
                        "path_file": path_file,
                        "name": name,
                        "st_atime": stat.st_atime,
                        "st_ctime": stat.st_ctime,
                        "st_mtime": stat.st_mtime,
                        "st_size": stat.st_size
                    })
                    if flag == unit:
                        flag = 0
                        await asyncio.sleep(0)

                for name in dirs:
                    flag += 1
                    dir_count += 1
                    path_dir = os.path.join(root, name)
                    stat = os.stat(path_dir)
                    files = os.listdir(path_dir)
                    if len(files) <= 0:
                        empty_dir += 1
                        empty_dir_list.append({
                            "path_dir": path_dir,
                            "name": name,
                            "st_atime": stat.st_atime,
                            "st_ctime": stat.st_ctime,
                            "st_mtime": stat.st_mtime,
                            "st_size": stat.st_size
                        })
                    if flag == unit:
                        flag = 0
                        await asyncio.sleep(0)

            size //= 1048576
            logger.info("path:{} size:{}MB file_count:{} dir_count:{} empty_dir:{}".format(path, size, file_count, dir_count, empty_dir))
            # 最远 -> 最近  最小 -> 最大
            file_list.sort(key=lambda k: (k.get('st_atime', 0), k.get('st_size', 0)))  #, reverse=True
            empty_dir_list.sort(key=lambda k: (k.get('st_mtime', 0), k.get('st_size', 0)))  #, reverse=True

            now = time.time()
            flag = 0
            unit = 100
            if default_timeouts <= 0:
                default_timeouts = g_settings.server.get("default_timeouts")
                if default_timeouts is None or default_timeouts <= 0:
                    default_timeouts = 300
            if ts_timeouts <= 0:
                ts_timeouts = g_settings.server.get("ts_timeouts")
                if ts_timeouts is None or ts_timeouts <= 0:
                    ts_timeouts = default_timeouts
            if m3u8_timeouts <= 0:
                m3u8_timeouts = g_settings.server.get("m3u8_timeouts")
                if m3u8_timeouts is None or m3u8_timeouts <= 0:
                    m3u8_timeouts = default_timeouts
            if timeout > 0:
                default_timeouts = ts_timeouts = m3u8_timeouts = timeout
            reupload_size = g_msg_queue_reupload.qsize()
            logger.info("{} reupload qsize:{}".format(path, reupload_size))
            # reupload_empty = False
            # if g_msg_queue_reupload.empty():
            #     reupload_empty = True
            # else:
            #     logger.info("{} reupload qsize:{}".format(path, g_msg_queue_reupload.qsize()))

            for file_info in file_list:
                if file_info["name"].endswith(".m3u8"):
                    # 备份的m3u8文件不能随意提交，可能覆盖新的列表导致丢失切片(闲时可以先下载线上文件，文件更大才提交)
                    if now - file_info["st_atime"] > m3u8_timeouts:
                        if mem != True and resend:
                            tmp = file_info["path_file"].split("/")
                            domain = tmp[-4]
                            appName = tmp[-3]
                            streamName = tmp[-2]
                            stream_key = "_".join([domain, appName, streamName])
                            result = await g_event_loop.run_in_executor(None, _download_worker, {
                                "domain": domain,
                                "appName": appName,
                                "streamName": streamName,
                                "path": file_info["path_file"],
                                "postfix":".tmp"
                            })
                            if result is not None:
                                local_stat = os.stat(file_info["path_file"])
                                online_stat = os.stat(file_info["path_file"] + ".tmp")
                                if local_stat.st_size > online_stat.st_size:
                                    if reupload_size < 10:
                                        await g_msg_queue_reupload.put({
                                            "domain": tmp[-4],
                                            "appName": tmp[-3],
                                            "streamName": tmp[-2],
                                            "path": file_info["path_file"]
                                        })
                                    else:
                                        logger.warning("skip reupload notify size:{}".format(g_msg_queue_reupload.qsize()))
                                else:
                                    logger.warning("m3u8_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                                    os.remove(file_info["path_file"])
                                    flag += 1
                                    if flag == unit:
                                        flag = 0
                                        await asyncio.sleep(0)
                            else:
                                if reupload_size < 10:
                                    await g_msg_queue_reupload.put({
                                        "domain": tmp[-4],
                                        "appName": tmp[-3],
                                        "streamName": tmp[-2],
                                        "path": file_info["path_file"]
                                    })
                                else:
                                    logger.warning("skip reupload notify size:{}".format(g_msg_queue_reupload.qsize()))
                        else:
                            logger.warning("m3u8_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                            os.remove(file_info["path_file"])
                            flag += 1
                            if flag == unit:
                                flag = 0
                                await asyncio.sleep(0)
                    else:
                        continue

                elif file_info["name"].endswith(".ts") and now - file_info["st_atime"] > ts_timeouts:
                    if resend:
                        tmp = file_info["path_file"].split("/")
                        domain = tmp[-4]
                        appName = tmp[-3]
                        streamName = tmp[-2]
                        stream_key = "_".join([domain, appName, streamName])
                        # obs_info = _check_obs_info(stream_key)

                        # 如果是内存目录剩余文件，先移动到磁盘，等磁盘监控触发重传
                        if mem:
                            if local_backup_dir is not None:
                                logger.warning("move:{} to {}".format(file_info["path_file"], local_backup_dir))
                                shutil.move(file_info["path_file"], "/".join([local_backup_dir, domain, appName, streamName]))
                                flag += 1
                                if flag == unit:
                                    flag = 0
                                    await asyncio.sleep(0)
                            else:
                                # 正常部署情况下以下不生效
                                await g_msg_queue_reupload.put({
                                    "mem": True,
                                    "domain": tmp[-4],
                                    "appName": tmp[-3],
                                    "streamName": tmp[-2],
                                    "path": file_info["path_file"]
                                })
                                logger.warning("mem reupload notify {} size:{}".format(file_info["path_file"], g_msg_queue_reupload.qsize()))
                        else:
                            if reupload_size < 10:
                                await g_msg_queue_reupload.put({"domain": domain, "appName": appName, "streamName": streamName, "path": file_info["path_file"]})
                            else:
                                logger.warning("skip reupload notify qsize:{}".format(g_msg_queue_reupload.qsize()))
                    else:
                        logger.warning("ts_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                        os.remove(file_info["path_file"])
                        flag += 1
                        if flag == unit:
                            flag = 0
                            await asyncio.sleep(0)

                elif now - file_info["st_atime"] > default_timeouts:
                    # 没有匹配后缀的文件，直接删除
                    logger.warning("default_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                    os.remove(file_info["path_file"])
                    flag += 1
                    if flag == unit:
                        flag = 0
                        await asyncio.sleep(0)
                # 已经排序，后面的都是没有超时的文件
                else:
                    break

            # 删除空目录，超过一天没有访问/最后一次修改超过三天
            for dir_info in empty_dir_list:
                if now - dir_info["st_mtime"] > 3 * 24 * 3600:  # now - dir_info["st_atime"] > 1 * 24 * 3600 and
                    logger.warning("default_timeouts:{} empty dir remove:{}".format(int(now - dir_info["st_mtime"]), dir_info["path_dir"]))
                    os.rmdir(dir_info["path_dir"])
                # 已经排序，后面的都是没有超时的文件
                else:
                    break
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        finally:
            await asyncio.sleep(default_timeouts / 2 + 1)



def _signal_reload(signalNumber, frame):
    try:
        global g_settings
        reload(scheduler_settings)
        g_settings = dict_to_Dict(scheduler_settings.configs)
        logger.info("settings reload:{}".format(g_settings))
    except Exception as e:
        logger.error(f"settings reload get exception:{e} trace:" + traceback.format_exc())





if __name__ == '__main__':
    """
    1、上传任务：接收TS、M3U8上传请求、加入上传队列
    2、UploadWorker读取上传任务，逐个上传文件，返回成功回调通知
    3、下载任务：接收下载请求，下载文件
    4、目录监控：内存目录、缓存目录、本地存储目录
    """
    try:
        logger.info('record_upload_server starting!')
        pes_update_config()
        signal.signal(signal.SIGHUP, _signal_reload)

        tasks = []
        # 心跳
        tasks.append(task_heartbeat())
        # 文件上传/下载服务
        tasks.append(task_record_upload_server())
        # 上传并行处理任务数
        for i in range(g_settings.server.upload_worker_count):
            tasks.append(task_record_upload_runner(i))
        # 重传任务
        tasks.append(task_record_reupload_runner(0))

        # 监控录制节点内存存储空间，超时移动到本地备用磁盘，重传
        tasks.append(
            task_dirs_monitor(g_settings.server.get("memory_dir"),
                                resend=True,
                                mem=True,
                                default_timeouts=g_settings.server.get("memory_default_timeouts"),
                                ts_timeouts=g_settings.server.get("memory_ts_timeouts"),
                                m3u8_timeouts=g_settings.server.get("memory_m3u8_timeouts")))
        # 监控本地重传备用磁盘，重传
        tasks.append(
            task_dirs_monitor(g_settings.server.get("local_backup_dir"),
                                resend=True,
                                mem=False,
                                default_timeouts=g_settings.server.get("local_backup_default_timeouts"),
                                m3u8_timeouts=g_settings.server.get("local_backup_m3u8_timeouts")))

        # 本地存储文件，由后处理控制删除，超过配置时间直接删除
        local_store_dir = g_settings.server.get("local_store_dir")
        local_store_timeouts = g_settings.server.get("local_store_timeouts")
        if local_store_dir is not None and os.path.exists(local_store_dir) and local_store_timeouts > 0:
            tasks.append(task_dirs_monitor(local_store_dir, resend=False, mem=False, timeout=local_store_timeouts))

        try:
            executor = ThreadPoolExecutor()  # max_workers=8
            g_event_loop.set_default_executor(executor)
            g_event_loop.run_until_complete(asyncio.wait(tasks))
            g_event_loop.close()
        except KeyboardInterrupt:
            logger.critical('<Got Signal: SIGINT, shutting down.>')
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())

    # obs_host = "obs-cn-shenzhen.yun.pingan.com"
    # bucket_name = "paavc-vod-publictest4"
    # obs_access_key = "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg"
    # obs_secret_key = "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q"
    # object_key = "test12345xxx99"
    # upload_content = "ttt12345"
    # local_file_upload_path = "/root/projects/record-upload/test-0-1609832620155.ts"
    # local_file_download_path = "/root/projects/record-upload/test-0-1609832620155.ts.download"

    # object_metadata = ObjectMetadata()
    # object_metadata.add_metadata("x-amz-date", get_gmt_time())
    # obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    # ret = obs.put_object(bucket_name, object_key, upload_content, object_metadata)
    # print(ret)

    # # obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    # s3_object = obs.get_object(bucket_name, object_key)
    # print(s3_object.get_object_stream().read().decode())

    # obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    # ret = obs.put_object_from_file(bucket_name, object_key, local_file_upload_path)
    # print(ret)

    # # obs = ObsOperator(obs_host, obs_access_key, obs_secret_key)
    # s3_object = obs.get_object(bucket_name, object_key)
    # with open(local_file_download_path, "wb") as to_file:
    #     s3_object.to_file(to_file)



# async def task_record_stream_update_e():
#     """
#     {
#         "action" : "RecordStreamUpdateE"

#     }
#     """
#     while True:
#         try:
#             body = await g_msg_queue_update.get()
#             async with aiohttp.ClientSession() as session:
#                 try:
#                     json_data = {"action": "RecordStreamUpdateE", "cluster_id": "", "ntype": "rd_upload", "device_id": "", "ip": "", "port": ""}
#                     async with session.post(g_settings.scheduler.api, json=json_data, timeout=5) as resp:
#                         pass
#                 except Exception as e:
#                     logger.error('heartbeat exception:%s' % e)
#                     logger.error("heartbeat trace:" + traceback.format_exc())

#         except ClientConnectionError:
#             logger.error('heartbeat connect to host error')
#         except Exception as e:
#             logger.error('heartbeat exception:%s' % e)
#             logger.error("heartbeat trace:" + traceback.format_exc())
#         finally:
#             await asyncio.sleep(0.5)


# logger_default = logging.getLogger()
# logger_default.setLevel(logging.WARNING)
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# host        | obs-cn-shenzhen.yun.pingan.com
# id          | 5
# bucket      | paavc-vod-publictest4
# access_key  | xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg
# secret_key  | JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q
# create_time | 1592893946864

# g_resend_file_list = []  # 需要重传的文件列表
# g_resend_wait_list = []  # 等待重传完毕的文件列表
# g_segment_upload_retry_timeout = 30  # 通知OBS传输，超时OBS传输超时时长
# g_segments_upload_retry_len = 5  # 触发TS重传的记录长度
# g_resend_maxbps_times_window = 30  # 统计码率时间窗口，单位秒
# g_resend_maxbps_ByteSize = (100 * 1024 * 1024 / 8) * g_resend_maxbps_times_window  # 重传最大阈值，超过以后认为上传服务异常了，后续要直接丢弃，避免磁盘IO洪峰
# g_recover_task = {}  # 记录恢复任务



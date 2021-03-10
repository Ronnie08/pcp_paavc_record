#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli
import os
import time
import re
import random
import shutil
import traceback
import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
import asyncio
from asyncio import QueueEmpty
import signal
import socket
import copy
import json
import aiohttp
from aiohttp import web
from aiohttp import ClientConnectionError
from importlib import reload

from obs import ObsOperator, ObjectMetadata, MultipartUploadFileRequest
from obs.util import *
from obs.exception import AmazonClientException
from concurrent.futures import ThreadPoolExecutor
from iobs_client import IOBSClient
from record_obs_hash import BucketHash
from record_media_m3u8_parse import M3U8Parser, M3u8Segment
import record_media_process_config as process_settings
from record_common import pes_get_configmap_str, pes_get_configmap_int, send_request_json, send_request_params, dict_to_Dict, run_command
from aredis_client import RedisClient

g_settings = dict_to_Dict(process_settings.configs)  # 服务启动配置
logging.basicConfig(
    level=logging.WARNING,
    format='[%(asctime)s-%(filename)s-%(lineno)d]:%(levelname)s: %(message)s',  # -%(funcName)s
    handlers=[logging.StreamHandler(),
              TimedRotatingFileHandler(filename=g_settings.server.log_file, when="MIDNIGHT", interval=1, backupCount=7)]  # S M H D MIDNIGHT
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

g_event_loop = asyncio.get_event_loop()

g_process_tasks = []  # 异步任务
g_process_queue = asyncio.Queue()  # 录制及后处理任务队列
g_upload_queue = asyncio.Queue()  # 文件上传任务队列
g_obs_bucket_hash = BucketHash()
g_min_cut_ms = 1500  # 最小裁剪单位ms
g_process_task_count = 0  # 当前正在执行的任务数
g_upload_task_count = 0  # 当前正在执行的任务数
g_redis_client = None
g_redis_prefix = "pard|{}|".format(g_settings.server.cluster_id)
g_redis_mprocess_task_list = g_redis_prefix + "mprocess_tlist"  # 录制任务队列名称

# mprocess_status
MPROCESS_STATUS_OK = 0
MPROCESS_STATUS_FAILD = 1
MPROCESS_STATUS_UPLOAD_ERROR = 2
MPROCESS_STATUS_DOWNLOAD_ERROR = 3
MPROCESS_STATUS_NO_DATA = 4
MPROCESS_STATUS_NO_KEY = 5


class FormatError(Exception):
    pass


def pes_update_config():
    pass
    # value = pes_get_configmap_str("/tmp/upload/temp_dir")
    # if value is not None:
    #     configs.server.temp_dir = value

    # value = pes_get_configmap_int("/tmp/upload/temp_dir_del_MB")
    # if value is not None:
    #     configs.server.temp_dir_del_MB = value


def _check_obs_info(stream_key):
    """
    直播TS切片数据存储，一致性HASH，分布于多个bucket中
    """
    try:
        return {
            "host": "obs-cn-shenzhen-internal.cloud.papub",  # 100.68.99.100   obs-cn-shenzhen.yun.pingan.com  obs-cn-shenzhen-internal.cloud.papub
            "bucket": g_obs_bucket_hash.getBucket(stream_key),
            "access_key": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
            "secret_key": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
        }
    except KeyError as e:
        logger.error("check obs exception:{} stream_key:{} trace:{}".format(e, stream_key, traceback.format_exc()))
        return None


async def task_heartbeat():
    """
    # 心跳上报
    # 当前任务队列长度
    # 当前设备CPU、磁盘IO
    {
        "action" : "Heartbeat"
        通用信息
        "cluster_id" : ""
        "ntype" : "rd_mprocess"
        "device_id" : ""
        "ip" : ""
        "port" : ""

        特定节点信息
        "process_qlen": 10,
        "upload_qlen": 10 
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
                        "ntype": "rd_mprocess",
                        "device_id": g_settings.server.device_id,
                        "ip": ipaddr,
                        "port": str(g_settings.server.service_port),
                        "process_count": g_process_queue.qsize() + g_process_task_count,
                        "upload_count": g_upload_queue.qsize() + g_upload_task_count
                    }
                    async with session.post(g_settings.scheduler.api, json=json_data, timeout=5) as resp:
                        await asyncio.sleep(10)
        except ClientConnectionError:
            logger.error('heartbeat connect to host error')
        except Exception as e:
            logger.error('heartbeat exception:%s' % e)
            logger.error("heartbeat trace:" + traceback.format_exc())
        finally:
            await asyncio.sleep(5)
    logger.error("(MsgNotify) task leave task_heartbeat")


def _sync_obs_upload_file(host, access_key, secret_key, bucket, object_key, path):
    try:
        if os.path.exists(path):
            obs = ObsOperator(host, access_key, secret_key)
            ret = obs.put_object_from_file(bucket, object_key, path)
            return ret
        else:
            logger.warning("upload path:{} is not exist".format(path))
            return None
    except Exception as e:
        logger.error("host:{} ak:{} sk:{} bucket:{} object_key:{}".format(host, access_key, secret_key, bucket, object_key))
        logger.error(f"obs upload get exception:{e} trace:" + traceback.format_exc())
        return None


async def _async_obs_upload_file(host, access_key, secret_key, bucket, object_key, path):
    try:
        result = await g_event_loop.run_in_executor(None, _sync_obs_upload_file, host, access_key, secret_key, bucket, object_key, path)
        if result is not None:  # and result.get_e_tag() is not None
            return result.get_e_tag()
        else:
            logger.warning("upload error:{}".format(result))
            return None
    except Exception as e:
        logger.error(f"obs upload get exception:{e} trace:" + traceback.format_exc())
        return None


async def _async_iobs_upload_file(host, access_key, secret_key, bucket, object_key, path, token=None):
    try:
        ret = None
        if os.path.exists(path):
            iobs_client = IOBSClient()
            if access_key is not None and secret_key is not None:
                host, bucket, token = iobs_client.get_iobs_upload_token(host, bucket, access_key, secret_key)
            if token is not None and len(token) > 0:
                ret = await iobs_client.upload(host, bucket, object_key, path, token)
            else:
                ret = await iobs_client.upload(host, None, object_key, path)
        return ret
    except Exception as e:
        logger.error("iobs upload host:{} ak:{} sk:{} bucket:{} object_key:{}".format(host, access_key, secret_key, bucket, object_key))
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        return ret


def _sync_download_file(body):
    """
    {
        "domain": "", 
        "appName": "", 
        "streamName": "", 
        "fileName": "", 

        "path": "", 
    }
    """
    try:
        object_key = None
        stream_key = "_".join([body["domain"], body["appName"], body["streamName"]])
        if body.get("fileName") is not None:
            object_key = "|".join([g_settings.server.get("cluster_id"), body["domain"], body["appName"], body["streamName"], body["fileName"]])
        else:
            object_key = "|".join([g_settings.server.get("cluster_id"), body["domain"], body["appName"], body["streamName"], body["path"].split("/")[-1]])
        obs_info = _check_obs_info(stream_key)
        obs = ObsOperator(obs_info["host"], obs_info["access_key"], obs_info["secret_key"])
        s3_object = obs.get_object(obs_info["bucket"], object_key)
        with open(body["path"], "wb") as to_file:
            s3_object.to_file(to_file)
        logger.info("{} {} download done {}".format(obs_info["bucket"], object_key, body["path"]))
        return body
    except Exception as e:
        if isinstance(e, AmazonClientException):  # isinstance(e, HTTPError) and e.code == 404
            logger.warning("download get an exception:{} obs_info:{} body:{} key:{}".format(e, obs_info, body, object_key))
            return None
        else:
            logger.error("download obs_info:{} body:{} key:{}".format(obs_info, body, object_key))
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
            return None


async def _async_download_file(body):
    try:
        return await g_event_loop.run_in_executor(None, _sync_download_file, body)
    except Exception as e:
        logger.error(f"download get exception:{e} trace:" + traceback.format_exc())
        return None


async def _download_m3u8_resource(index, body):
    """
    下载m3u8资源
     {
        "startTime" : "111111111111",
        "endTime" : "222222222222",

        "domain" : "www.test.com", 
        "appName" : "live", 
        "streamName" : "test001",
        "fileName" : "xxx.m3u8",

        "path" : "/media/.../xxx.m3u8",
        "mprocess_task_id" : ""
    }
    """
    # 下载M3U8文件
    stream_path = os.path.join(g_settings.server.work_path, body["domain"], body["appName"], body["streamName"], body["mprocess_task_id"])
    os.makedirs(stream_path, exist_ok=True)
    body["fileName"] = "{}.m3u8".format(body["streamName"])
    body["path"] = "{}/{}.m3u8".format(stream_path, body["streamName"])

    ret = await _async_download_file(body)
    if ret is None:
        logger.warning("download {} failed".format(body))
        return None

    # 解析M3U8文件 -> 根据开始结束时间，解析M3U8文件，全量下载/切断式部分下载(生成新的m3u8文件) 下载列表
    m3u8_parse = M3U8Parser(body["path"], start=body["startTime"], end=body["endTime"])
    # TEST 跳过资源下载
    ts_list = m3u8_parse.get_ts_list()
    task_list = list()
    for file_name in ts_list:
        if len(task_list) < g_settings.server.process_download_worker_count:
            fbody = copy.copy(body)
            fbody["fileName"] = file_name
            fbody["path"] = os.path.join(stream_path, file_name)
            task_list.append(asyncio.create_task(_async_download_file(fbody)))

        if len(task_list) >= g_settings.server.process_download_worker_count:
            tasks = asyncio.gather(*task_list, return_exceptions=True)
            await tasks
            task_list.clear()
    if len(task_list) > 0:
        tasks = asyncio.gather(*task_list, return_exceptions=True)
        await tasks
        task_list.clear()

    for file_name in ts_list:
        fpath = os.path.join(stream_path, file_name)
        if not os.path.exists(fpath):
            logger.error("stream:{} lost:{}".format(m3u8_parse.get_m3u8_path(), fpath))

    return m3u8_parse


async def _process_m3u8_resource(index, body, m3u8_parse):
    """
    媒体后处理，m3u8资源转MP4
     {
        "startTime" : "111111111111",
        "endTime" : "222222222222",

        "domain" : "www.test.com", 
        "appName" : "live", 
        "streamName" : "test001",

        "mprocess_task_id" : ""
    }
    {
        "m3u8Path":""
    }
    """
    # 执行转封装
    # 如果失败，做异常检测及异常处理(补充静默数据/转码/拼接/转分辨率等)
    # 执行完成，删除过程中的文件资源
    mprocess_task_id = body.get("mprocess_task_id")
    start_time = body["startTime"]
    end_time = body["endTime"]

    # 媒资开始及结束时间
    media_start = m3u8_parse.get_start_timems()
    media_end = m3u8_parse.get_end_timems()
    media_duration = m3u8_parse.get_duration_timems()
    duration = int(media_duration / 1000)
    media_source_m3u8 = m3u8_parse.get_m3u8_path()
    media_source_flist = m3u8_parse.get_flist_path()
    media_ts_list = m3u8_parse.get_ts_list()
    # logger.info(media_ts_list)

    # 起始偏移时间 -ss 1:23.456
    ss = 0
    if start_time - media_start > g_min_cut_ms:
        ss = int((start_time - media_start) / 1000)
        duration -= ss
    # 结尾偏移时间  -t 0:14.678   -to 2:18.963
    to = 0
    if media_end - end_time > g_min_cut_ms:
        to = int((media_duration - (media_end - end_time) + 999) / 1000)
        duration = to - ss
    logger.info("tid:{} p:{} start_time:{} end_time:{} media_start:{} media_end:{}".format(mprocess_task_id, index, start_time, end_time, media_start,
                                                                                           media_end))
    logger.info("tid:{} p:{} timelen:{} media_duration:{} ss:{} to:{} duration:{}".format(mprocess_task_id, index, int(end_time - start_time), media_duration,
                                                                                          ss, to, duration))

    # 如果定时录制，实际数据不足需要调整返回的数据值
    if media_start > start_time:
        body["startTime"] = media_start
        logger.info("tid:{} p:{} change start_time to media_start offset:+{}".format(mprocess_task_id, index, (media_start - start_time)))
    if media_end < end_time:
        body["endTime"] = media_end
        logger.info("tid:{} p:{} change end_time to media_end offset:-{}".format(mprocess_task_id, index, (end_time - media_end)))

    # 其他参数
    random_value = random.randint(0, 1000000)
    is_trans = body.get("isTrans")
    out_file_name = '{}-{}-{}-{}-{}.mp4'.format(body['appName'], body['streamName'], start_time, duration, random_value)
    tmp_out_file_name = '{}-{}-{}-{}-{}_tmp.mp4'.format(body['appName'], body['streamName'], start_time, duration, random_value)
    cwd = os.path.join(g_settings.server.work_path, body["domain"], body["appName"], body["streamName"])
    out_file_path = os.path.join(cwd, out_file_name)
    tmp_out_file_path = os.path.join(cwd, tmp_out_file_name)

    # 构造转封装指令，直接剪切M3U8文件（连续流/无需切割）
    if m3u8_parse.is_discontinuity() is not True or (ss == 0 and to == 0):
        trans_command = [g_settings.server.ffmpeg, '-analyzeduration', '20000000', '-v', 'warning']
        if ss > 0:
            trans_command.extend(['-ss', str(ss)])
        if to > 0:
            trans_command.extend(['-to', str(to)])
        trans_command.extend(['-i', media_source_m3u8])

        if is_trans is None or is_trans == 0:
            trans_command_param = '-c copy -movflags faststart -y'
        else:
            trans_command_param = '-vcodec copy -acodec aac -ar 44100 -bsf:a aac_adtstoasc -movflags faststart -y'
        trans_command.extend(trans_command_param.split())
        trans_command.append(out_file_path)
        logger.info("tid:{} p:{} {}".format(mprocess_task_id, index, ' '.join(trans_command)))
        trans_result = await run_command(*trans_command, is_stdout=False, cwd=cwd)
        if len(trans_result) > 0:
            logger.warning("tid:{} p:{} trans_result:{}".format(mprocess_task_id, index, trans_result))
            # TODO 异常任务处理
            # 开始有音频没视频/有视频没音频
            # 音视频参数变化
        else:
            logger.info(f"tid:{mprocess_task_id} p:{index} trans ok")
        return out_file_path
    # 构造转封装指令，使用文件拼接方式
    else:
        trans_command = [g_settings.server.ffmpeg, '-analyzeduration', '20000000', '-v', 'warning', '-safe', '0', '-f', 'concat', '-i', media_source_flist]
        if is_trans is None or is_trans == 0:
            trans_command_param = '-c copy -movflags faststart -y'
        else:
            trans_command_param = '-vcodec copy -acodec aac -ar 44100 -bsf:a aac_adtstoasc -movflags faststart -y'
        trans_command.extend(trans_command_param.split())

        if ss == 0 and to == 0:
            trans_command.append(out_file_path)
            logger.info("{} p:{} {}".format(mprocess_task_id, index, ' '.join(trans_command)))
            trans_result = await run_command(*trans_command, is_stdout=False, cwd=cwd)
            if len(trans_result) > 0:
                logger.warning("tid:{} p:{} trans_result:{}".format(mprocess_task_id, index, trans_result))
            else:
                logger.info(f"tid:{mprocess_task_id} p:{index} trans ok")
        else:
            trans_command.append(tmp_out_file_path)
            logger.info("tid:{} p:{} {}".format(mprocess_task_id, index, ' '.join(trans_command)))
            trans_result = await run_command(*trans_command, is_stdout=False, cwd=cwd)
            if len(trans_result) > 0:
                logger.warning("tid:{} p:{} trans_result:{}".format(mprocess_task_id, index, trans_result))
            else:
                logger.info(f"tid:{mprocess_task_id} p:{index} trans ok")

            convert_command = [g_settings.server.ffmpeg, '-v', 'warning']
            if ss > 0:
                convert_command.extend(['-ss', str(ss)])
            if to > 0:
                convert_command.extend(['-to', str(to)])
            convert_command_param = '-i {} -c copy -movflags faststart -y'.format(tmp_out_file_path)
            convert_command.extend(convert_command_param.split())
            convert_command.append(out_file_path)
            logger.info("tid:{} p:{} {}".format(mprocess_task_id, index, ' '.join(convert_command)))
            trans_result = await run_command(*convert_command, is_stdout=False)
            if len(trans_result) > 0:
                logger.warning("tid:{} p:{} trans_result:{}".format(mprocess_task_id, index, trans_result))
                # TODO 异常任务处理
                # 开始有音频没视频/有视频没音频
                # 音视频参数变化
            else:
                logger.info(f"tid:{mprocess_task_id} p:{index} trans ok")

            # 删除中间文件
            os.remove(tmp_out_file_path)
        return out_file_path


async def _get_media_info(index, body, process_file):
    mprocess_task_id = body.get("mprocess_task_id")
    media_info = {"size": 0, "duration": 0, "codecType": "audio", "width": 0, "height": 0, "fps": "0", "vcodec": "h264", "vb": "0", "acodec": "aac", "ab": "0"}

    probe_command = [g_settings.server.ffprobe, '-v', 'quiet', '-print_format', 'json', '-show_error', '-show_format', '-show_streams', process_file]
    logger.info("tid:{} p:{} {}".format(mprocess_task_id, index, ' '.join(probe_command)))
    probe_result = await run_command(*probe_command, is_stdout=True)
    if probe_result:
        probe_result = json.loads(probe_result)
        if probe_result is None:
            logger.warning(f"tid:{mprocess_task_id} ffprobe {process_file} error.")
            return None
        elif probe_result.get("format") is not None and probe_result['format']['nb_streams'] != 0:
            media_info['duration'] = float(probe_result['format']['duration'])
            media_info['size'] = probe_result['format']['size']
            for stream in probe_result['streams']:
                if stream['codec_type'] == 'video':
                    media_info['vcodec'] = stream['codec_name']
                    media_info['vb'] = int(stream['bit_rate']) // 1000
                    media_info['width'] = int(stream['coded_width'])
                    media_info['height'] = int(stream['coded_height'])
                    media_info['codecType'] = 'video'
                    frame_rate = stream['r_frame_rate']
                    fps_list = frame_rate.split('/', 1)
                    if int(fps_list[1]) > 0:
                        media_info['fps'] = str(round(float(fps_list[0]) / float(fps_list[1]), 2))
                    else:
                        media_info['fps'] = stream['r_frame_rate']
                elif stream['codec_type'] == 'audio':
                    media_info['acodec'] = stream['codec_name']
                    media_info['ab'] = int(stream['bit_rate']) // 1000
            return media_info
        else:
            logger.warning(f"tid:{mprocess_task_id} ffprobe {process_file} error. {probe_result}")
            return None
    else:
        logger.warning(f"tid:{mprocess_task_id} ffprobe return nothing")
        return None


async def _upload_process_resource(body):
    """
    上传后处理后的媒体资源
    """
    # 上传 OBS / IOBS / VOD-obs / IOT-iobs
    ret = None
    target = body.get("target")
    host = body.get("host")
    bucket = body.get("bucketName")
    access_key = body.get("accessKey")
    secret_key = body.get("secretKey")
    token = body.get("token")
    object_key = body.get("objectKey")
    upload_process_file = body.get("upload_process_file")
    if not os.path.exists(upload_process_file):
        logger.warning("upload file:{} not exist")
        return ret
    if object_key is None or len(object_key) <= 0:
        src = "-".join([body["domain"], body["appName"], body["streamName"], str(body["startTime"]), str(body["endTime"])])
        object_key = body["streamName"] + "_" + str(hashlib.md5(src.encode(encoding='UTF-8')).hexdigest()) + ".mp4"
        body["objectKey"] = object_key

    if target == "OBS":
        if host is None or len(host) <= 0:
            if g_settings.get("obs") is not None:
                host = g_settings.obs.get("default_host")
        ret = await _async_obs_upload_file(host, access_key, secret_key, bucket, object_key, upload_process_file)
    elif target == "IOBS":
        if host is None or len(host) <= 0:
            if g_settings.get("iobs") is not None:
                host = g_settings.iobs.get("default_host")
        ret = await _async_iobs_upload_file(host, access_key, secret_key, bucket, object_key, upload_process_file, token)
    elif target == "IOBS-IOT":
        if host is None or len(host) <= 0:
            if g_settings.get("iobs_iot") is not None:
                host = g_settings.iobs_iot.get("default_host")
        ret = await _async_iobs_upload_file(host, None, None, None, object_key, upload_process_file, token=None)
    else:
        logger.warning("upload target error:{}".format(body))

    if ret is not None:
        logger.info(f"upload:{upload_process_file} to target:{target} host:{host} bucket:{bucket} okey:{object_key} done")
        logger.info(f"download link:http://{bucket}.obs-cn-shenzhen.pinganyun.com/{object_key}")
        return body
    else:
        return ret


async def _update_status(status, body=None, mprocess_task_id=None):
    # 更新任务状态
    if mprocess_task_id is not None:
        ret, value = await g_redis_client.get(mprocess_task_id)
        if ret != 0 or value is None:
            logger.warning(f"tid:{mprocess_task_id} get task info error(redis)")
        else:
            body = json.loads(value)

    if body is not None:
        mprocess_task_id = body.get("mprocess_task_id")
        body["action"] = "UpdateMediaProcessTask"
        body["mprocess_status"] = status
        ret = await g_redis_client.set(mprocess_task_id, json.dumps(body))
        if ret != 0:
            logger.warning(f"update task status error(redis)")

        # 通知调度中心，录制结束，状态更新
        status, text = await send_request_json(g_settings.scheduler.mprocess,
                                               json_data={
                                                   "action": "UpdateMediaProcessTask",
                                                   "mprocess_task_id": mprocess_task_id
                                               })
        if status != 200:
            logger.warning(f"tid:{mprocess_task_id} done cbk error")
    return None


async def task_media_process_worker(index):
    """
    循环处理录制任务
    1、下载录制资源m3u8文件列表
    2、分析m3u8文件，找到最小下载切片区间
    """
    global g_process_task_count
    timeout = 0.2 + 0.1 * (index + 1)
    while True:
        try:
            update_status = MPROCESS_STATUS_OK
            m3u8_parse = body = None
            if g_process_queue.empty():
                ret, mprocess_task_id = await g_redis_client.blpop(g_redis_mprocess_task_list, timeout=1)
                if ret != 0:
                    await asyncio.sleep(timeout)
                    continue
                if mprocess_task_id is None:
                    continue
                logger.info(f"tid:{mprocess_task_id} p:{index} get task")
                ret, value = await g_redis_client.get(mprocess_task_id)
                if ret != 0 or value is None:
                    logger.warning(f"tid:{mprocess_task_id} p:{index} get task info error(redis)")
                    continue
                body = json.loads(value)
            else:
                try:
                    body = g_process_queue.get_nowait()
                except QueueEmpty:
                    continue

            mprocess_task_id = body.get("mprocess_task_id")
            if mprocess_task_id is None or len(mprocess_task_id) <= 0:
                logger.warning(f"tid:{mprocess_task_id} p:{index} is none error")
            g_process_task_count += 1
            logger.info(f"tid:{mprocess_task_id} p:{index} get process:{body}")

            # 下载M3U8资源
            m3u8_parse = await _download_m3u8_resource(index, body)
            if m3u8_parse is None or len(m3u8_parse.get_ts_list()) <= 0:
                logger.warning(f"p:{index} download:{body} error.")
                update_status = MPROCESS_STATUS_NO_DATA
                continue
            logger.info(f"tid:{mprocess_task_id} p:{index} resource download done")

            # 后处理
            process_file = await _process_m3u8_resource(index, body, m3u8_parse)
            if process_file is None:
                logger.warning(f"p:{index} download:{body} process error.")
                update_status = MPROCESS_STATUS_FAILD
                continue
            logger.info(f"tid:{mprocess_task_id} p:{index} resource process done:{process_file}")

            # get media info
            info = await _get_media_info(index, body, process_file)
            if info is None:
                logger.warning(f"p:{index} get media info error:{process_file}")
                if process_file is not None and os.path.exists(process_file):
                    os.remove(process_file)
                update_status = MPROCESS_STATUS_FAILD
                continue
            logger.info(f"tid:{mprocess_task_id} p:{index} get media info done:{process_file} {info}")

            body["upload_process_file"] = process_file
            body["media_info"] = info
            await g_upload_queue.put(body)
        except Exception as e:
            logger.error("tid:{} p:{} exception:{} trace:{}".format(mprocess_task_id, index, e, traceback.format_exc()))
            if process_file is not None and os.path.exists(process_file):
                os.remove(process_file)
            update_status = MPROCESS_STATUS_FAILD
        finally:
            # 删除过程文件
            if m3u8_parse is not None:
                m3u8_parse.remove_all()
                logger.info(f"tid:{mprocess_task_id} remove tmp files")
            if body is not None:
                g_process_task_count -= 1
            # 更新任务状态
            if update_status != MPROCESS_STATUS_OK:
                await _update_status(update_status, body)
                logger.warning(f"(MsgNotify) mprocess error status:{update_status} tid:{mprocess_task_id} p:{index} ")
    logger.error(f"(MsgNotify) task leave task_media_process_worker {index}")


async def task_media_upload_worker(index):
    """
    循环处理录制任务
    3、执行媒体后处理(转封装/音频转码)
    4、删除过程文件
    """
    global g_upload_task_count
    while True:
        try:
            body = await g_upload_queue.get()
            mprocess_task_id = body.get("mprocess_task_id")
            g_upload_task_count += 1
            logger.info(f"tid:{mprocess_task_id} u:{index} get upload:{body}")

            # 上传文件
            upload_process_file = body.get("upload_process_file")
            done_info = await _upload_process_resource(body)
            if done_info is None:
                logger.warning(f"(MsgNotify) upload error tid:{mprocess_task_id} u:{index} upload:{upload_process_file} body:{body}")
                # 更新任务状态
                await _update_status(MPROCESS_STATUS_UPLOAD_ERROR, body)
                continue

            # 更新任务状态
            await _update_status(MPROCESS_STATUS_OK, done_info)
            logger.info(f"tid:{mprocess_task_id} u:{index} upload:{upload_process_file} done")
        except Exception as e:
            logger.error(f"tid:{mprocess_task_id} u:{index} get exception:{e} trace:" + traceback.format_exc())
        finally:
            # 删除过程文件
            if upload_process_file is not None and os.path.exists(upload_process_file):
                os.remove(upload_process_file)
            else:
                logger.warning(f"tid:{mprocess_task_id} u:{index} {upload_process_file} can't remove")
            g_upload_task_count -= 1
    logger.error("(MsgNotify) task leave task_media_upload_worker")


async def _api_logger_factory(app, handler):
    async def logger(request):
        # logger.info('Request: %s %s' % (request.method, request.path))
        return await handler(request)

    return logger


async def _handle_record_media_process(request):
    """
    {
        "mode":"1/2",
        "startTime" : "111111111111",
        "endTime" : "222222222222",

        "domain" : "www.test.com", 
        "appName" : "live", 
        "streamName" : "test001",

        "objectKey":"", 
        "host":"",
        "bucketName":"",
        "aKey":"",
        "sKey":"",
        "token":"",

        # "streams" : [{"domain" : "www.test.com", "appName" : "live", "streamName" : "test001"}, ...]
        # "others": {"appId":"直播系统下发对应VOD系统", "appKey":"类似token密钥", "bucketName":"", "accountInfo":"", "commonParam":"", ...},
        # "process" : {
        #                 "acodec":"", 
        #                 "samplerate":"",
        #                 "ab":"",

        #                 "vcodec":"", 
        #                 "fps":"", 
        #                 "gop":"", 
        #                 "width":"", 
        #                 "height":"", 

        #                 "format":"mp4", 
        #             }
    }
    """
    try:
        body = await request.json()
        logger.info(body)
        await g_process_queue.put(body)
        return web.json_response({'code': 20000, "msg": "OK"})
    except KeyError as e:
        logger.error("api excpetion:{} trace:{}".format(e, traceback.format_exc()))
        return web.json_response({'code': 40000, "msg": "Failed"})


async def task_media_process_server():
    try:
        # app = web.Application(loop=g_event_loop, middlewares=[_api_logger_factory])
        app = web.Application()
        app.add_routes([web.post(g_settings.server.api_live_record, _handle_record_media_process)])
        app_runner = web.AppRunner(app)
        await app_runner.setup()
        srv = await g_event_loop.create_server(app_runner.server, g_settings.server.service_ip, g_settings.server.service_port)
        logger.info('http live record server started!')
        return srv
        # site = web.TCPSite(app_runner, 'localhost', 8080)
        # await site.start()
    except Exception as e:
        logger.error(f"task_media_process_server get exception:{e} trace:" + traceback.format_exc())


# async def task_media_process_worker(index=0):
#     try:
#        # get task from redis list

#        pass
#     except Exception as e:
#         logger.error(f"task_media_process_start get exception:{e} trace:" + traceback.format_exc())


async def task_dirs_monitor(path=None):
    """
    监控目录及目录下所有文件，超时清理删除，避免磁盘空间数据堆积
    """
    logger.info("path:{}".format(path))
    while path is not None:
        try:
            flag = 0
            size = 0
            file_count = 0
            dir_count = 0
            empty_dir = 0

            unit = 100
            file_list = []
            dir_list = []
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
                        dir_list.append({
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
            dir_list.sort(key=lambda k: (k.get('st_mtime', 0), k.get('st_size', 0)))  #, reverse=True

            now = time.time()
            flag = 0
            unit = 100
            default_timeouts = g_settings.monitor.get("default_timeouts")
            ts_timeouts = g_settings.monitor.get("ts_timeouts")
            if ts_timeouts is None:
                ts_timeouts = default_timeouts
            m3u8_timeouts = g_settings.monitor.get("m3u8_timeouts")
            if m3u8_timeouts is None:
                m3u8_timeouts = default_timeouts
            mp4_timeouts = g_settings.monitor.get("mp4_timeouts")
            if mp4_timeouts is None:
                mp4_timeouts = default_timeouts

            for file_info in file_list:
                if file_info["name"].endswith(".ts") and now - file_info["st_atime"] > ts_timeouts:
                    logger.warning("ts_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                    os.remove(file_info["path_file"])
                    flag += 1
                    if flag == unit:
                        flag = 0
                        await asyncio.sleep(0)

                elif file_info["name"].endswith(".m3u8") and now - file_info["st_atime"] > m3u8_timeouts:
                    logger.warning("m3u8_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
                    os.remove(file_info["path_file"])
                    flag += 1
                    if flag == unit:
                        flag = 0
                        await asyncio.sleep(0)

                elif file_info["name"].endswith(".mp4") and now - file_info["st_atime"] > mp4_timeouts:
                    logger.warning("mp4_timeouts:{} remove:{}".format(int(now - file_info["st_atime"]), file_info["path_file"]))
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
            for dir_info in dir_list:
                if dir_info["path_dir"] == os.path.join(path, 'dvr') or dir_info["path_dir"] == os.path.join(path, 'hls'):
                    continue
                if now - dir_info["st_mtime"] > 3 * 24 * 3600:
                    logger.warning("empty dir timeout:{} remove:{}".format(int(now - dir_info["st_mtime"]), dir_info["path_dir"]))
                    os.rmdir(dir_info["path_dir"])
                # 已经排序，后面的都是没有超时的文件
                else:
                    break
        except Exception as e:
            logger.error(f"get exception:{e} trace:" + traceback.format_exc())
        finally:
            await asyncio.sleep(g_settings.monitor.get("loop_times") + 1)
    logger.error(f"(MsgNotify) task leave task_dirs_monitor {path}")


async def test_task():
    # # test task  单次推流，长时间中断后拼接 - OBS
    # await g_process_queue.put({
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611227815087,
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
    # })

    # # test task  单次推流，长时间中断后拼接 - IOBS
    # await g_process_queue.put({
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611227815087,
    #     "domain": "www.test.com",
    #     "appName": "live",
    #     "streamName": "ok",
    #     "objectKey": "",
    #     "target": "IOBS",
    #     "host": "stg-iobs-upload.pingan.com.cn",
    #     "bucketName": "iobs-dmz-dev",
    #     "accessKey": "60Kd0F6CMV60KCWYdCd02IWW6220dYJK",
    #     "secretKey": "WCKJV0WI898DYF6DDV9KdIIdCd6I0M2W",
    #     "token": "",
    # })

    # test task  单次推流，长时间中断后拼接 - IOBS-IOT
    # await g_process_queue.put({
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611227815087,
    #     "domain": "www.test.com",
    #     "appName": "live",
    #     "streamName": "ok",
    #     "objectKey": "",
    #     "target": "IOBS-IOT",
    #     "host": "stg-iobs-upload.pingan.com.cn",
    # })

    # test task  连续流 - IOBS-IOT
    # await g_process_queue.put({
    #     "startTime": 1611212411659 + 15000,
    #     "endTime": 1611212753626 - 250000,
    #     "domain": "www.test.com",
    #     "appName": "live",
    #     "streamName": "ok",
    #     "objectKey": "",
    #     "target": "IOBS-IOT",
    #     "host": "stg-iobs-upload.pingan.com.cn",
    # })

    task_info = {
        "action": "CreateMediaProcessTask",
        "startTime": 1614332280000,
        "endTime": 1614332300000,
        "domain": "push.localhost",
        "appName": "live",
        "streamName": "ok02265",
        "objectKey": "",
        "target": "OBS",
        "host": "obs-cn-shenzhen.yun.pingan.com",
        "bucketName": "paavc-vod-publictest4",
        "accessKey": "xt5CkKNR8ZVDmPusVWgzd4SFe7u00tCBU05yqjEnD8Zve4jCo-Gv1xgvGwGnkigTIBczyJ0MIrGsSsqQ6O9YTg",
        "secretKey": "JOGUZ6EG2E7VdOimQMOCaFF8zemvNkygYBv68s2BRqARHMooJpPo9mNb8683_pNxdfXpgn2QROBtlLQEl_pi9Q",
        "token": "",
        "mprocess_task_id": "123"
    }
    await g_process_queue.put(task_info)
    pass


def _signal_reload(signalNumber, frame):
    try:
        global g_settings
        reload(process_settings)
        g_settings = dict_to_Dict(process_settings.configs)
        logger.info("settings reload:{}".format(g_settings))
    except Exception as e:
        logger.error(f"settings reload get exception:{e} trace:" + traceback.format_exc())


if __name__ == '__main__':
    try:
        logger.info('record_media_process_server starting!')
        pes_update_config()
        signal.signal(signal.SIGHUP, _signal_reload)

        # 连接redis
        g_redis_client = RedisClient(g_settings.redis.host, g_settings.redis.port, g_settings.redis.password, g_settings.redis.cluster)
        g_redis_client.connect_to_redis()

        # 启动录制服务
        g_process_tasks.append(task_media_process_server())

        # 监控本地工作目录
        g_process_tasks.append(task_dirs_monitor(g_settings.server.get("work_path")))
        # 启动心跳任务
        g_process_tasks.append(task_heartbeat())

        # 启动后处理worker
        process_worker_count = g_settings.server.get("process_worker_count")
        if isinstance(process_worker_count, int) and process_worker_count > 0:
            for i in range(process_worker_count):
                g_process_tasks.append(task_media_process_worker(i))
        else:
            g_process_tasks.append(task_media_process_worker(0))

        # 启动媒体上传worker
        upload_worker_count = g_settings.server.get("upload_worker_count")
        if isinstance(upload_worker_count, int) and upload_worker_count > 0:
            for i in range(upload_worker_count):
                g_process_tasks.append(task_media_upload_worker(i))
        else:
            g_process_tasks.append(task_media_upload_worker(0))

        # 测试
        # g_process_tasks.append(test_task())

        try:
            executor = ThreadPoolExecutor()  # max_workers=8
            g_event_loop.set_default_executor(executor)
            g_event_loop.run_until_complete(asyncio.wait(g_process_tasks))
            g_event_loop.close()
        except KeyboardInterrupt:
            logger.critical('<Got Signal: SIGINT, shutting down.>')
    except Exception as e:
        logger.error(f"get exception:{e} trace:" + traceback.format_exc())

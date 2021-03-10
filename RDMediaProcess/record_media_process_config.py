#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: liaoli
'''
Default configurations.
'''

configs = {
    'server': {
        'cluster_id': 'urdHnDev',
        'device_id': 'rd_mprocess_01',
        'service_ip': '127.0.0.1',
        'service_port': 8000,
        'api_live_record': '/record/live/mprocess',
        'process_worker_count': 5,
        'process_download_worker_count': 3,
        'upload_worker_count': 3,
        'log_file': '/log/mprocess/debug_liaoli_mprocess.log',
        'work_path': "/mnt/data",

        'ffmpeg':'/usr/local/bin/ffmpeg',
        'ffprobe':'/usr/local/bin/ffprobe',
    },
    'scheduler': {
        'api': 'http:// 10.10.96.13:18080/scheduler/api/v1',
        'mprocess': 'http:// 10.10.96.13:18080/scheduler/mprocess/v1',
    },
    'monitor': {
        'default_timeouts': 43200,
        'ts_timeouts': 7200,
        'm3u8_timeouts': 7200,
        'mp4_timeouts': 7200,
        'loop_times': 3600
    },
    'redis': {
        # 'host': 'localhost',
        # 'port': 6379,
        # 'password': '',
        # 'cluster': False
        'host': 'rd8gb675.redis.db.cloud.papub',
        'port': 11573,
        'password': 'RDRedisDev2021',
        'cluster': True
    },
    'obs': {
        'default_host':"obs-cn-shenzhen.yun.pingan.com"
    },
    'iobs': {
        'default_host':""
    },
    'iobs_iot': {
        'default_host':""
    }
}

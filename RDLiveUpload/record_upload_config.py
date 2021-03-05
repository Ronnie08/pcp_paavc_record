#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

'''
Default configurations.
'''

configs = {
    'server': {
        'cluster_id': 'Default_Cluster',
        'device_id': 'rd_upload_01',
        'service_ip': '127.0.0.1',
        'service_port': 9101,
        'obs_upload': '/upload',
        'obs_download': '/download',
        'obs_recover': '/recover',
        'upload_worker_count': 8,                # 上传并发执行任务数

        'memory_dir' : '/dev/shm/data',          # 实时监控内存目录
        'memory_default_timeouts' : 300,         # 内存目录监控(异常结束等情况会有残留的无效数据，定时清理)
        'memory_ts_timeouts' : 180,
        'memory_m3u8_timeouts' : 864000,

        'local_backup_dir' : '/mnt/data',        # 用于内存目录上传以上后缓存数据重试
        'local_backup_reupload_count' : 3,       # 最大重传次数
        'local_backup_default_timeouts' : 300,   # 目录扫描遍历时间间隔
        'local_backup_m3u8_timeouts' : 280,

        'log_file': '/log/upload/debug_liaoli_upload.log',
    },
    'scheduler': {
        'api': 'http://127.0.0.1:9190/scheduler/api/v1',
    }
}

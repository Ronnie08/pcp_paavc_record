#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

configs = {
    'server': {
        'cluster_id': 'Default_Cluster',
        'scheduler_id': 'debug_liaoli_1',
        'scheduler_ip': '0.0.0.0',
        'scheduler_port': 9190,
        'scheduler_api_path': '/scheduler/api/v1',
        'scheduler_mprocess_path': '/scheduler/mprocess/v1',

        'log_file': '/log/scheduler/debug_liaoli_scheduler.log',
        'max_record_nbstreams': 150,
        'heartbeat_interval': 10
        # 'report_log_file': '/log/scheduler/debug_liaoli_report.log',
        # 'thread_num': 10,
        # 'max_publish_nbstreams': 100,
    },
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'password': '',
        'cluster': False
        # 'host': 'rd8gb675.redis.db.cloud.papub',
        # 'port': 11573,
        # 'password': 'RDRedisDev2021'
        # 'cluster': True
    },
    'manager':{
        'callback':''
    }
}

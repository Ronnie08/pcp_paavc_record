#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli

import record_scheduler_config
from importlib import reload

class Dict(dict):
    def __init__(self,names=(), values=(), **kw):
        super(Dict,self).__init__(**kw)
        for k, v in zip(names,values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


def toDict(d):
    D = Dict()
    for k, v in d.items():
        D[k] = toDict(v) if isinstance(v,dict) else v

    return D

def custom_reload():
    global configs
    reload(record_scheduler_config)
    configs = record_scheduler_config.configs
    # try:
    #     import record_scheduler_config
    # except ImportError:
    #     pass
    configs = toDict(configs)


configs = {}
custom_reload()


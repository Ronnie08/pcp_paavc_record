#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli
import logging
import asyncio
import aiohttp
from aiohttp import ClientConnectionError

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


def dict_to_Dict(d):
    D = Dict()
    for k, v in d.items():
        D[k] = dict_to_Dict(v) if isinstance(v, dict) else v

    return D


def pes_get_configmap_str(key_path):
    value = None
    try:
        if os.path.exists(key_path) and os.path.isfile(key_path):
            with open(key_path) as f:
                value = f.read(256)
                value.strip()
                logger.info("get_configmap str key:{} -> value:{}".format(key_path, value))
    except Exception as e:
        logger.error('key_path:{} error!'.format(key_path))
    return value


def pes_get_configmap_int(key_path):
    value = None
    try:
        if os.path.exists(key_path) and os.path.isfile(key_path):
            with open(key_path) as f:
                value = f.read(256)
                value.strip()
                logger.info("get_configmap int key:{} -> value:{}".format(key_path, value))
                return int(value)
    except Exception as e:
        logger.error('key_path:{} error!'.format(key_path))
    return value


async def run_command(*args, is_stdout, cwd=None):
    # os.chdir(user_path)  , cwd=path
    process = await asyncio.create_subprocess_exec(*args,
                                                   stdin=asyncio.subprocess.PIPE,
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE,
                                                   cwd=cwd)
    logger.info('Started:' + '(pid = ' + str(process.pid) + ')')
    stdout, stderr = await process.communicate()
    # os.chdir(os.path.dirname(user_path))
    if process.returncode == 0:
        logger.info('Done:' + '(pid = ' + str(process.pid) + ')')
    else:
        logger.error('Failed:' + '(pid = ' + str(process.pid) + ')')

    if is_stdout:
        result = stdout.decode().strip()
        return result
    else:
        result = stderr.decode().strip()
        # strip mp4 Non-monotonous DTS in output stream
        lines = result.split("\n")
        result = list()
        for line in lines:
            if line.find("Non-monotonous DTS in output stream") != -1:
                continue
            result.append(line)
        return "\n".join(result)


async def run_command_shell(cmd, is_stdout, cwd=None):
    try:
        process = await asyncio.create_subprocess_shell(cmd,
                                                        stdin=asyncio.subprocess.PIPE,
                                                        stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE,
                                                        cwd=cwd)
        logger.info('Started:' + '(pid = ' + str(process.pid) + ')')
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            logger.info('Done:' + '(pid = ' + str(process.pid) + ')')
        else:
            logger.error('Failed:' + '(pid = ' + str(process.pid) + ')')
    except Exception as e:
        logger.error("run_command_shell get an exception {}".format(e))
    if is_stdout:
        result = stdout.decode().strip()
    else:
        result = stderr.decode().strip()
    return result


async def send_request_json(url, json_data, header=None, timeout=5):
    try:
        logger.info(f"send json:{url} header:{header} data:{json_data}")
        http_headers = {}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=json_data, headers=header, timeout=timeout) as resp:
                status = resp.status
                text = await resp.text()
                logger.debug("Response status:{} data:{}".format(status, text))
                return (status, text)
    except ClientConnectionError:
        logger.error('send request json Cannot connect to host')
        return (-1, '')
    except Exception as e:
        logger.error(f'send request json exception:{e} trace:' + traceback.format_exc())
        return (-1, '')


async def send_request_params(url, params_data, header=None, timeout=5):
    try:
        logger.info(f"send params:{url} header:{header} data:{params_data}")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=params_data, headers=header, timeout=timeout) as resp:
                status = resp.status
                text = await resp.text()
                logger.debug("Response status:{} data:{}".format(status, text))
                return (status, text)
    except ClientConnectionError:
        logger.error('send request json Cannot connect to host')
        return (-1, '')
    except Exception as e:
        logger.error('send request params exception:%s' % e)
        return (-1, '')

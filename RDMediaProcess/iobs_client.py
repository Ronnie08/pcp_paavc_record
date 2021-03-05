#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author: liaoli
import os
import asyncio
import aiohttp
import aiofile
from aiohttp import FormData
from aiofile import AIOFile, Writer, Reader
import logging
import traceback
import base64
import hashlib
import json
import time
import hmac
# import ssl
import requests
# centos7 ssl.SSLError: ('No cipher can be selected.',) 
# requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"  # ubuntu 20.04 -> ssl:default [[SSL: DH_KEY_TOO_SMALL] dh key too small (_ssl.c:1123)]

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(filename)s-%(lineno)d-%(funcName)s -> %(message)s',
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IOBSClientException(Exception):
    pass


class IOBSClientRetry(Exception):
    """
    token 过期，跟新token后重试
    """
    pass


class IOBSClient:
    def __init__(self, mulBlock=1, blockSize=4, chunkSize=256, getToken=None):
        """
        mulBlock: 单文件大于4MB后，分块并发上传数
        blockSize: 文件分块大小，单位MB
        chunkSize: 文件分片大小，单位KB
        """
        self.mulBlock = mulBlock
        self.blockSize = blockSize * 1024 * 1024
        self.chunkSize = chunkSize * 1024
        self.getToken = getToken
        pass

    def get_iobs_upload_token(self, host, bucketName, ak, sk):
        # host = "stg-iobs-upload.pingan.com.cn"
        # bucketName = "iobs-dmz-dev"
        # ak = "60Kd0F6CMV60KCWYdCd02IWW6220dYJK"
        # sk = "WCKJV0WI898DYF6DDV9KdIIdCd6I0M2W"
        sk_byte = hashlib.sha1(sk.encode('utf-8')).digest()

        msg = {"scope": bucketName, "deadline": int(time.time() + 15 * 60)}  # bucketName:key
        msg_byte = json.dumps(msg).replace(" ", "").encode('utf-8')
        msg_sign = base64.b64encode(msg_byte).decode('utf-8').replace("+", "-").replace("/", "_")

        hmac_sign = hmac.new(sk_byte, msg_sign.encode('utf-8'), hashlib.sha1).digest()
        sign = base64.b64encode(hmac_sign).decode('utf-8').replace("+", "-").replace("/", "_")

        token = f"{ak}:{sign}:{msg_sign}"
        logger.info(f"upload host:{host} bucket:{bucketName} token:{token}")
        return host, bucketName, token

    def get_iobs_download_token(self, host, bucketName, ak, sk, objectKey):
        # host = "stg-iobs-upload.pingan.com.cn"
        # bucketName = "iobs-dmz-dev"
        # ak = "60Kd0F6CMV60KCWYdCd02IWW6220dYJK"
        # sk = "WCKJV0WI898DYF6DDV9KdIIdCd6I0M2W"
        exp = int(time.time() + 15*60)
        base_url = "http://{host}/download/{bucketName}/{objectKey}?e={exp}"

        sk_byte = hashlib.sha1(sk.encode('utf-8')).digest()

        hmac_sign = hmac.new(sk_byte, base_url.encode('utf-8'), hashlib.sha1).digest()
        sign = base64.b64encode(hmac_sign).decode('utf-8').replace("+", "-").replace("/", "_")

        token = f"{ak}:{sign}"
        logger.info(f"download host:{host} bucket:{bucketName} token:{token}")
        return host, bucketName, token, objectKey


    def _get_iot_token(self):
        try:
            url = 'https://test-api.pingan.com.cn:20443/oauth/oauth2/access_token'
            response = requests.post(url,
                                     headers={"Accept": "application/json"},
                                     json={
                                         "client_id": "P_OMP_CALL_RBSS",
                                         "client_secret": "x6RTR64K",
                                         "grant_type": "client_credentials"
                                     })
            if response.status_code == 200:
                info = response.json()
                url = 'https://test-api.pingan.com.cn:20443/open/appsvr/it/api/upload/app/mediaIobsAction/getToken'
                response = requests.post(url,
                                         headers={
                                             "Accept": "application/json",
                                             "Content-Type": "application/x-www-form-urlencoded",
                                             "X-Authorization": "D9D3416B-E1F5-4C43-889A-A4A80D71FAE3"
                                         },
                                         data="access_token={}".format(info["data"]["access_token"]))
                if response.status_code == 200:
                    info = response.json()
                    bucketName = info["data"]["body"]["iobs_bucket"]
                    token = info["data"]["body"]["iobs_token"]
                    logger.info(f"IOT bucket:{bucketName} token:{token}")
                    return token, bucketName
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            return None, None

    async def _upload_small_file(self, host, bucketName, objectKey, path, token):
        try:
            data = FormData()
            data.add_field('token', token)
            data.add_field('file', open(path, 'rb'), filename=path.split("/")[-1], content_type='application/octet-stream')

            url = f'http://{host}/upload/{bucketName}/{objectKey}'
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as resp:
                    if resp.status == 200:
                        ret = await resp.text()
                        logger.info(ret)
                        return json.loads(ret)
                    else:
                        logger.warning("upload:{} {}".format(resp.status, await resp.text()))
                        return None
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            return None

    async def _upload_slice(self, host, bucketName, path, body, offset):
        """
        """
        pass

    async def _create_block(self, host, bucketName, objectKey, token, count):
        """
        """
        try:
            blockList = list()
            async with aiohttp.ClientSession() as session:
                for i in range(count):
                    index = i + 1
                    url = f'http://{host}/mkblk/{bucketName}/{objectKey}/{index}?token={token}'
                    async with session.post(url) as resp:
                        if resp.status == 200:
                            block = await resp.text()  # await resp.json()
                            blockList.append(json.loads(block))
                            logger.debug(block)
                        else:
                            logger.warning("mkblk:{} {}".format(resp.status, await resp.text()))
            return blockList
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            return blockList

    async def _upload_block(self, host, bucketName, objectKey, path, token, body):
        """
        """
        try:
            offset = 0
            context = body
            md5 = hashlib.md5()
            async with aiohttp.ClientSession() as session:
                async with AIOFile(path, 'rb') as afp:
                    blockId = body.get("blockId")
                    reader = Reader(afp, offset=self.blockSize * (int(blockId) - 1), chunk_size=self.chunkSize)
                    while offset < self.blockSize:
                        chunk = await reader.read_chunk()
                        if len(chunk) > 0:
                            logger.debug("context:{} offset:{}".format(context, offset))
                            md5.update(chunk)
                            context = f"{context}".encode("utf-8")
                            context = str(base64.b64encode(context), "utf-8")
                            url = f'http://{host}/bput/{bucketName}/{objectKey}/{context}/{offset}?token={token}'
                            async with session.post(url, data=chunk) as resp:
                                if resp.status == 200:
                                    context = await resp.text()  # await resp.json()
                                    offset += len(chunk)
                                elif resp.status == 401:
                                    return "update_token_retry"
                                else:
                                    logger.warning("bput:{} {}".format(resp.status, await resp.text()))
                                    return None
                        else:
                            break
            return md5.hexdigest()
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            return None

    async def _mkfile_block(self, host, bucketName, objectKey, token, filename, md5, size):
        try:
            url = f'http://{host}/mkfile/{bucketName}/{objectKey}?token={token}&fileName={filename}&fileSize={size}&md5={md5}'
            async with aiohttp.ClientSession() as session:
                async with session.post(url) as resp:
                    if resp.status == 200:
                        ret = await resp.text()  # await resp.json()
                        logger.info(ret)
                        return json.loads(ret)
                    else:
                        logger.warning("mkfile:{} {}".format(resp.status, await resp.text()))
                        return None
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            return None

    async def upload(self, host, bucketName, objectKey, path, token=None):
        """
        host:  接入点
        bucketName: bucket 名称
        objectKey: 上传文件 桶唯一值
        path: 文件路径
        token: 鉴权信息token
        """
        try:
            if os.path.exists(path) is not True:
                return
            if token is None:
                # loop = asyncio.get_running_loop() Just used after python 3.7
                loop = asyncio.get_event_loop()
                token, bucket = await loop.run_in_executor(None, self._get_iot_token)
                if token is None:
                    return None
                if bucketName is None:
                    bucketName = bucket
                elif bucketName != bucket:
                    bucketName = bucket
                    logger.warning(f"change bucket from:{bucketName} to:{bucket}")

            stat = os.stat(path)
            if stat.st_size <= self.blockSize:
                return await self._upload_small_file(host, bucketName, objectKey, path, token)
            else:
                blockList = await self._create_block(host, bucketName, objectKey, token, count=int((stat.st_size + self.blockSize - 1) / self.blockSize))
                md5Mix = ''
                for block in blockList:
                    while True:
                        retry_count = 0
                        md5 = await self._upload_block(host, bucketName, objectKey, path, token, block)
                        if md5 is not None:
                            if "update_token_retry" == md5:
                                # token过期 更新续传
                                retry_count += 1
                                # loop = asyncio.get_running_loop() Just used after python 3.7
                                loop = asyncio.get_event_loop()
                                token, bucket = await loop.run_in_executor(None, self._get_iot_token)
                                if retry_count < 3:
                                    continue
                                else:
                                    logger.warning(f"retry_count:{retry_count} error {block}")
                                    break
                            else:
                                md5Mix += str(md5)
                                break
                        else:
                            return None

                md5Mix = str(hashlib.md5(md5Mix.encode(encoding='UTF-8')).hexdigest())
                filename = path.split("/")[-1]
                return await self._mkfile_block(host, bucketName, objectKey, token, filename, md5Mix, stat.st_size)
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            # raise IOBSClientException(e)
            return None

    async def download(self, host, bucketName, objectKey, path, token=None):
        try:
            async with aiohttp.ClientSession() as session:
                url = f'http://{host}/download/{bucketName}/{objectKey}?attname=&token={token}'
                async with session.get(url) as r:
                    async with AIOFile(path, 'wb') as afp:
                        writer = Writer(afp)
                        result = await r.read()
                        await writer(result)
                        await afp.fsync()
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))
            # raise IOBSClientException(e)
            return None


if __name__ == "__main__":
    def get_test_info():
        host = "stg-iobs-upload.pingan.com.cn"
        bucketName = "iobs-dmz-dev"
        ak = "60Kd0F6CMV60KCWYdCd02IWW6220dYJK"
        sk = "WCKJV0WI898DYF6DDV9KdIIdCd6I0M2W"

        sk_byte = hashlib.sha1(sk.encode('utf-8')).digest()

        msg = {"scope": bucketName, "deadline": int(time.time() + 15 * 60)}  # bucketName:key
        msg_byte = json.dumps(msg).replace(" ", "").encode('utf-8')
        msg_sign = base64.b64encode(msg_byte).decode('utf-8').replace("+", "-").replace("/", "_")

        sign = hmac.new(sk_byte, msg_sign.encode('utf-8'), hashlib.sha1).digest()
        sign = base64.b64encode(sign).decode('utf-8').replace("+", "-").replace("/", "_")

        token = f"{ak}:{sign}:{msg_sign}"
        logger.info(f"DEV -> host:{host} bucket:{bucketName} token:{token}")
        return host, bucketName, token

    def get_iot_test_info():
        # curl -H "Content-Type:application/json" -X POST --data '{"client_id":"P_OMP_CALL_RBSS", "client_secret":"x6RTR64K", "grant_type":"client_credentials"}' https://test-api.pingan.com.cn:20443/oauth/oauth2/access_token
        url = 'https://test-api.pingan.com.cn:20443/oauth/oauth2/access_token'
        response = requests.post(url,
                                 headers={"Accept": "application/json"},
                                 json={
                                     "client_id": "P_OMP_CALL_RBSS",
                                     "client_secret": "x6RTR64K",
                                     "grant_type": "client_credentials"
                                 })
        if response.status_code == 200:
            info = response.json()
            # {'data': {'access_token': '8A22EC4D62F748819C22...00487739DD', 'expires_in': '8621', 'openid': 'P_OMP_CALL_RBSS00'}, 'msg': '', 'ret': '0'}

            url = 'https://test-api.pingan.com.cn:20443/open/appsvr/it/api/upload/app/mediaIobsAction/getToken'
            response = requests.post(url,
                                     headers={
                                         "Accept": "application/json",
                                         "Content-Type": "application/x-www-form-urlencoded",
                                         "X-Authorization": "D9D3416B-E1F5-4C43-889A-A4A80D71FAE3"
                                     },
                                     data="access_token={}".format(info["data"]["access_token"]))
            # response = {
            #     "ret": "0",
            #     "msg": "",
            #     "requestId": "null",
            #     "data": {
            #         "code": "0000",
            #         "message": "OK",
            #         "body": {
            #             "iobs_bucket":
            #             "icore-claim-dmz-dev-pri",
            #             "iobs_token":
            #             "2I6DJI6DJWW0VdCWIKIM8d6CYKDK8ICY:t-F1eWW55LChbREeeD_BqlwJWS8=:eyJzY29wZSI6Imljb3JlLWNsYWltLWRtei1kZXYtcHJpIiwiZGVhZGxpbmUiOjE2MTA1MjQ3NTV9"
            #         }
            #     }
            # }
            if response.status_code == 200:
                info = response.json()
                bucketName = info["data"]["body"]["iobs_bucket"]
                token = info["data"]["body"]["iobs_token"]
                host = "stg-iobs-upload.pingan.com.cn"
                logger.info(f"IOT -> host:{host} bucket:{bucketName} token:{token}")
                return host, bucketName, token
            else:
                return None, None, None

    async def test_upload(host, bucketName, objectKey, path, token):
        iobsClient = IOBSClient(blockSize=4)
        await iobsClient.upload(host, bucketName, objectKey, path, token)


    async def test_upload_download():
        host = "stg-iobs-upload.pingan.com.cn"
        bucketName = "iobs-dmz-dev"
        ak = "60Kd0F6CMV60KCWYdCd02IWW6220dYJK"
        sk = "WCKJV0WI898DYF6DDV9KdIIdCd6I0M2W"
        objectKey = "testupload"
        path = "./testupload"

        iobsClient = IOBSClient(blockSize=4)
        host, bucketName, token = iobsClient.get_iobs_upload_token(host, bucketName, ak, sk)
        await iobsClient.upload(host, bucketName, objectKey, path, token)

        path = "./testupload.download"
        host, bucketName, token, objectKey = iobsClient.get_iobs_download_token(host, bucketName, ak, sk, objectKey)
        await iobsClient.download(host, bucketName, objectKey, path, token)



    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_upload_download())

    # host, bucketName, token = get_iot_test_info()
    # info = token.split(":")[-1]
    # token_info = base64.b64decode(info).decode("utf-8")
    # logger.info(f"token_info:{token_info}")
    # objectKey = "testupload"
    # path = "./testupload"
    # loop.run_until_complete(test_upload(host, bucketName, objectKey, path, token))

    # host, bucketName, token = get_test_info()
    # info = token.split(":")[-1]
    # token_info = base64.b64decode(info).decode("utf-8")
    # logger.info(f"token_info:{token_info}")
    # objectKey = "testupload"
    # path = "./testupload"
    # loop.run_until_complete(test_upload(host, bucketName, objectKey, path, token))

    # objectKey = "testupload2"
    # path = "./testupload2"
    # loop.run_until_complete(test_upload(host, bucketName, objectKey, path, token))

    # objectKey = "testupload3"
    # path = "./testupload3"
    # loop.run_until_complete(test_upload(host, bucketName, objectKey, path, token))

    loop.close()

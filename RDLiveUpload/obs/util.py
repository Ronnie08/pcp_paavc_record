#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import hashlib
import base64
from .constants import *
from .sign import RGWSigner


def get_gmt_time():
    return datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")


def get_request_headers(object_metadata):
    # 处理日期header，如果配置了x-amz-date，则删除掉date
    if HTTP_X_AMZ_DATE in object_metadata.get_metadata():
        del object_metadata.get_metadata()[HTTP_DATE]

    headers = {}
    for k, v in object_metadata.get_metadata().items():
        headers[k] = v
    for k, v in object_metadata.get_user_metadata().items():
        headers[S3_USER_METADATA_PREFIX + k] = v
    return headers


'''
服务器端读取header时，如果存在x_amz_date则把其转换date，后面读取date的时候再去读取x_amz_date的值（原因不知道）
所以签名的时候key始终是date而不是x_amz_date，而request传输的时候需要x_amz_date而不是date, 因此
sdk在签名时需要把x_amz_date替换为date，request传输时把date替换为x_amz_date
'''


def replace_header_date_to_x_amz_date(headers):
    # if HTTP_DATE in headers:
    #     headers[S3_ALTERNATE_DATE] = headers[HTTP_DATE]
    #     del headers[HTTP_DATE]
    return headers


def replace_header_x_amz_date_to_date(headers):
    # if S3_ALTERNATE_DATE in headers:
    #     headers[HTTP_DATE] = headers[S3_ALTERNATE_DATE]
    #     del headers[S3_ALTERNATE_DATE]
    return headers


def trim_tag(e_tag):
    if e_tag.startswith('\"'):
        e_tag = e_tag[1:]
    if e_tag.endswith('\"'):
        e_tag = e_tag[:-1]
    return e_tag


def base64_md5(data):
    b_md5 = hashlib.md5(data).digest()
    b64_md5 = base64.b64encode(b_md5)
    return b64_md5.decode(DEFAULT_ENCODING)


def md5_hex_digest_from_file(file_path):
    assert isinstance(file_path, str), 'file_path must be a str'
    buffer_size = 5 * 1024 * 1024
    m = hashlib.md5()
    with open(file_path, "rb") as file_object:
        while True:
            chunk = file_object.read(buffer_size)
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()


def get_authorization(passport, request_path, http_method, headers):
    signature = RGWSigner.sign(passport.get_obs_secret_key(), request_path, http_method, headers)
    return "AWS" + " " + passport.get_obs_access_key() + ":" + signature




#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import hmac
from hashlib import sha1
from .constants import *
# from .util import replace_header_x_amz_date_to_date


class RGWSigner(object):

    @staticmethod
    def sign(obs_secret_key, request_path, http_method, headers=None):
        assert obs_secret_key, 'obs_secret_key is empty'
        assert request_path, 'request_path is empty'
        assert http_method, 'http_method is empty'
        if headers is None:
            headers = {}
        # replace_header_x_amz_date_to_date(headers)
        string_to_sign = RGWSigner._get_s3_canonical_string(http_method, request_path, headers)
        return RGWSigner.compute_sign(obs_secret_key, string_to_sign)

    @staticmethod
    def compute_sign(obs_secret_key, string_to_sign):
        b_string_to_sign = bytes(string_to_sign, encoding=DEFAULT_ENCODING)
        b_key = bytes(obs_secret_key, encoding=DEFAULT_ENCODING)

        h_mac_sign = hmac.new(b_key, b_string_to_sign, sha1).digest()
        signature = base64.b64encode(h_mac_sign)
        return signature.decode(DEFAULT_ENCODING)

    @staticmethod
    def _get_s3_canonical_string(http_method, request_path, headers):
        sign_headers = {}
        RGWSigner._compute_sign_headers(sign_headers, headers)
        RGWSigner._make_sure_required_headers(sign_headers)
        return RGWSigner._compute_sign_string(http_method, request_path, sign_headers)

    @staticmethod
    def _compute_sign_headers(sign_headers, headers):
        for k, v in headers.items():
            if k == HTTP_CONTENT_TYPE or k == HTTP_CONTENT_MD5 or k == HTTP_DATE or k.startswith(AMAZON_PREFIX):
                sign_headers[k] = v

    @staticmethod
    def _make_sure_required_headers(sign_headers):
        if S3_ALTERNATE_DATE in sign_headers:
            sign_headers[HTTP_DATE] = ""
        if HTTP_CONTENT_TYPE not in sign_headers:
            sign_headers[HTTP_CONTENT_TYPE] = ""
        if HTTP_CONTENT_MD5 not in sign_headers:
            sign_headers[HTTP_CONTENT_MD5] = ""

    @staticmethod
    def _compute_sign_string(http_method, request_path, sign_headers):
        string_to_sign = http_method + "\n"
        list_sign_header_names = sorted(sign_headers)
        for k in list_sign_header_names:
            if k.startswith(AMAZON_PREFIX):
                string_to_sign += k.lower() + ":"
            if sign_headers[k]:
                string_to_sign += sign_headers[k]
            string_to_sign += "\n"
        string_to_sign += request_path
        return string_to_sign

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib.request
from _io import BufferedReader
from http.client import HTTPResponse
from urllib.parse import quote_plus
import requests
from .operation import MultipartUploadOperation
from .model import *
from .util import *


class MultipartUploadFileRequest(object):

    def __init__(self):
        self._bucket_name = None
        self._object_key = None
        self._upload_file_path = None
        self._upload_notifier = None
        self._part_size = 5 * 1024 * 1024
        self._enable_checkpoint = True
        self._enable_md5 = True
        self._checkpoint_file_path = None
        self._retry_time = 0
        self._object_metadata = ObjectMetadata()

    def set_bucket_name(self, bucket_name):
        assert isinstance(bucket_name, str), 'bucket_name must be an str'
        self._bucket_name = bucket_name

    def set_object_key(self, object_key):
        assert isinstance(object_key, str), 'object_key must be an str'
        self._object_key = object_key

    def set_upload_file_path(self, upload_file_path):
        assert isinstance(upload_file_path, str), 'upload_file_path must be an str'
        self._upload_file_path = upload_file_path

    def set_upload_notifier(self, upload_notifier):
        self._upload_notifier = upload_notifier

    def set_part_size(self, part_size):
        assert isinstance(part_size, int), 'part_size must be an int'
        self._part_size = part_size

    def set_enable_checkpoint(self, enable_checkpoint):
        assert isinstance(enable_checkpoint, bool), 'enable_checkpoint must be an bool'
        self._enable_checkpoint = enable_checkpoint

    def set_enable_md5(self, enable_md5):
        assert isinstance(enable_md5, bool), 'enable_md5 must be an bool'
        self._enable_md5 = enable_md5

    def set_checkpoint_file_path(self, checkpoint_file_path):
        assert isinstance(checkpoint_file_path, str), 'checkpoint_file_path must be an str'
        self._checkpoint_file_path = checkpoint_file_path

    def set_retry_time(self, retry_time):
        assert isinstance(retry_time, int), 'retry_time must be an int'
        if retry_time < 0:
            retry_time = 0
        if retry_time > 3:
            retry_time = 3
        self._retry_time = retry_time

    def set_object_metadata(self, object_metadata):
        assert isinstance(object_metadata, ObjectMetadata), 'object_metadata must be an ObjectMetadata'
        assert object_metadata, 'object_metadata is empty'
        self._object_metadata = object_metadata

    def get_bucket_name(self):
        return self._bucket_name

    def get_object_key(self):
        return self._object_key

    def get_upload_file_path(self):
        return self._upload_file_path

    def get_upload_notifier(self):
        return self._upload_notifier

    def get_part_size(self):
        return self._part_size

    def is_enable_checkpoint(self):
        return self._enable_checkpoint

    def is_enable_md5(self):
        return self._enable_md5

    def get_checkpoint_file_path(self):
        return self._checkpoint_file_path

    def get_retry_time(self):
        return self._retry_time

    def get_object_metadata(self):
        return self._object_metadata


class RGWPassport(object):

    def __init__(self, obs_host, obs_access_key, obs_secret_key):
        self._obs_host = obs_host
        self._obs_access_key = obs_access_key
        self._obs_secret_key = obs_secret_key

    def get_obs_host(self):
        return self._obs_host

    def get_obs_access_key(self):
        return self._obs_access_key

    def get_obs_secret_key(self):
        return self._obs_secret_key


class ObsOperator(object):

    def __init__(self, obs_host, obs_access_key, obs_secret_key):
        assert obs_host, 'obs_host is empty'
        assert obs_access_key, 'obs_access_key is empty'
        assert obs_secret_key, 'obs_secret_key is empty'
        self._passport = RGWPassport(obs_host, obs_access_key, obs_secret_key)

    def put_object(self, bucket_name, object_key, data, object_metadata=None):
        assert bucket_name, 'bucket_name is empty'
        assert object_key, 'object_key is empty'
        assert data, 'data is empty'
        assert isinstance(data, (str, BufferedReader, HTTPResponse)), 'data must be one of str,' \
                                                                      ' _io.BufferedReader, http.client.HTTPResponse'
        if object_metadata is None:
            object_metadata = ObjectMetadata()

        if isinstance(data, HTTPResponse):
            return self._put_object_from_network_stream(bucket_name, object_key, data, object_metadata)
        return self._put_object(bucket_name, object_key, data, object_metadata)

    def put_object_from_file(self, bucket_name, object_key, from_file_path, object_metadata=None):
        assert from_file_path, 'from_file_path is empty'
        assert isinstance(from_file_path, (str, BufferedReader)), 'from_file_path must be one of str, ' \
                                                                  '_io.BufferedReader'
        if isinstance(from_file_path, str):
            with open(from_file_path, 'rb') as from_file:
                return self.put_object(bucket_name, object_key, from_file, object_metadata)
        return self.put_object(bucket_name, object_key, from_file_path, object_metadata)

    def put_object_multipart(self, upload_file_request):
        try:
            multipart_operation = MultipartUploadOperation(self._passport)
            multipart_operation.upload(upload_file_request)
        except Exception as e:
            logging.exception(e)
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    def get_object(self, bucket_name, object_key):
        assert bucket_name, 'bucket_name is empty'
        assert object_key, 'object_key is empty'
        try:
            gmt_time = get_gmt_time()

            object_metadata = ObjectMetadata()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, APPLICATION_X_WW_FORM_URLENCODED)
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(object_key)
            request_path = r"/%s/%s" % (bucket_name, object_key)
            authorization = get_authorization(self._passport, request_path, "GET", headers)
            headers[HTTP_AUTHORIZATION] = authorization
            replace_header_date_to_x_amz_date(headers)

            url = ObsOperator._get_url(self._passport.get_obs_host(), request_path)
            request = urllib.request.Request(url)
            for k, v in headers.items():
                request.add_header(k, v)
            object_stream = urllib.request.urlopen(request)
            return S3Object(object_stream)
        except Exception as e:
            logging.exception(e)
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    @staticmethod
    def _get_url(_obs_host, request_path):
        return r"http://%s%s" % (_obs_host, request_path)

    def _put_object(self, bucket_name, object_key, data, object_metadata):
        try:
            gmt_time = get_gmt_time()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, APPLICATION_OCTET_STREAM)
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(object_key)
            request_path = r"/%s/%s" % (bucket_name, object_key)
            authorization = get_authorization(self._passport, request_path, "PUT", headers)

            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization

            url = ObsOperator._get_url(self._passport.get_obs_host(), request_path)
            response = requests.put(url=url, data=data, headers=headers, verify=False)
            if response.status_code != 200:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
            return PutObjectResult(response.headers[HTTP_E_TAG], response.headers[HTTP_DATE])
        except Exception as e:
            logging.exception(e)
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    def _put_object_from_network_stream(self, bucket_name, object_key, data, object_metadata):
        files = {
            'file': (object_key, data)
            # 'file': data
        }
        try:
            gmt_time = get_gmt_time()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, MULTIPART_FORM_DATA)
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(object_key)
            request_path = r"/%s/%s" % (bucket_name, object_key)
            authorization = get_authorization(self._passport, request_path, "POST", headers)

            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization
            del headers[HTTP_CONTENT_TYPE]

            url = ObsOperator._get_url(self._passport.get_obs_host(), request_path)
            response = requests.post(url=url, data={}, headers=headers, files=files, verify=False)
            if response.status_code != 200:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
            return PutObjectResult(response.headers[HTTP_E_TAG], response.headers[HTTP_DATE])
        except Exception as e:
            logging.exception(e)
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

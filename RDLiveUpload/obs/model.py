#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import shutil
from _io import BufferedWriter
from .exception import *
from .constants import *


class PutObjectResult(object):

    def __init__(self, e_tag, date, object_key=""):
        assert e_tag, 'e_tag is empty'
        assert date, 'date is empty'

        self._e_tag = e_tag
        self._date = date
        self._object_key = object_key

    def get_e_tag(self):
        return self._e_tag

    def get_date(self):
        return self._date

    def get_object_key(self):
        return self._object_key

    def __str__(self):
        return 'PutObjectResult (e_tag: %s, date: %s, object_key: %s)' % (self._e_tag, self._date, self._object_key)

    __repr__ = __str__


class ObjectMetadata(object):
    def __init__(self):
        self._metadata = {}
        self._user_metadata = {}

    def add_metadata(self, key, value):
        assert isinstance(key, str), 'key must be an str'
        assert isinstance(value, str), 'value must be an str'
        if key and value:
            self._metadata[key] = value

    def add_user_metadata(self, key, value):
        assert isinstance(key, str), 'key must be an str'
        assert isinstance(value, str), 'value must be an str'
        if key and value:
            self._user_metadata[key] = value

    def get_metadata(self):
        return self._metadata

    def get_user_metadata(self):
        return self._user_metadata


class S3Object(object):

    def __init__(self, object_stream):
        self._object_stream = object_stream
        self._object_metadata = ObjectMetadata()

        self._read_object_metadata()

    def get_object_stream(self):
        return self._object_stream

    def get_object_metadata(self):
        return self._object_metadata

    def to_file(self, file):
        assert file, 'file is empty'
        assert isinstance(file, BufferedWriter), 'file must be an _io.BufferedWriter'
        try:
            shutil.copyfileobj(self._object_stream, file)
        except Exception as e:
            logging.exception(e)
            raise AmazonClientException(e)

    def _read_object_metadata(self):
        prefix_len = len(S3_USER_METADATA_PREFIX)
        headers = self._object_stream.getheaders()
        for item in headers:
            key = item[0]
            value = item[1]
            if key.startswith(S3_USER_METADATA_PREFIX):
                key = key[prefix_len:]
                self._object_metadata.add_user_metadata(key, value)
            else:
                self._object_metadata.add_metadata(key, value)

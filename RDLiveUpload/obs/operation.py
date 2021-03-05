#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import pickle
from _io import BufferedReader
from enum import Enum
from urllib.parse import quote_plus
import requests
from .util import *
from .model import *
from .xml_parse import CommonXmlResponseHandler

# MIN_SIZE_PER_PART = 5 * 1024 * 1024
# MAX_SIZE_PER_PART = 2 * 1024 * 1024 * 1024
# MAX_PARTS = 10000

MIN_SIZE_PER_PART = 5 * 1024 * 1024
MAX_SIZE_PER_PART = 100 * 1024 * 1024
MAX_PARTS = 204800


class State(Enum):
    before_init = 1
    inited = 2
    before_upload = 3
    part_uploaded = 4
    before_complete = 5
    completed = 6


class MultipartUploadOperation(object):

    def __init__(self, passport):
        self._passport = passport

    def upload(self, upload_file_request):
        assert upload_file_request.get_bucket_name(), 'bucket_name is empty'
        assert upload_file_request.get_object_key(), 'object_key is empty'
        assert upload_file_request.get_upload_file_path(), 'upload_file_path is empty'
        if not upload_file_request.get_checkpoint_file_path():
            upload_file_request.set_checkpoint_file_path(upload_file_request.get_upload_file_path() + ".ucp")
        upload_check_point = UploadCheckPoint()

        if not upload_file_request.is_enable_checkpoint():
            self._prepare_multipart_upload(upload_check_point, upload_file_request)
            upload_check_point.store(upload_file_request.get_checkpoint_file_path())
        else:
            try:
                upload_check_point.load(upload_file_request.get_checkpoint_file_path())
                if upload_file_request.get_object_key() != upload_check_point.object_key or \
                        upload_check_point.is_valid(upload_file_request.get_upload_file_path()):
                    MultipartUploadOperation._remove_file(upload_file_request.get_checkpoint_file_path())
                    if upload_check_point.upload_id:
                        # 清理分片
                        self._abort_upload(upload_check_point, upload_file_request)
                    self._prepare_multipart_upload(upload_check_point, upload_file_request)
                    upload_check_point.store(upload_file_request.get_checkpoint_file_path())
            except Exception as e:
                MultipartUploadOperation._remove_file(upload_file_request.get_checkpoint_file_path())
                if upload_check_point.upload_id:
                    # 清理分片
                    self._abort_upload(upload_check_point, upload_file_request)
                self._prepare_multipart_upload(upload_check_point, upload_file_request)
                upload_check_point.store(upload_file_request.get_checkpoint_file_path())

        MultipartUploadOperation._notify_upload(State.inited, upload_check_point, upload_file_request)
        self._upload_multiparts(upload_check_point, upload_file_request)
        self._complete_multipart(upload_check_point, upload_file_request)

    def _prepare_multipart_upload(self, upload_check_point, upload_file_request):
        upload_check_point.magic = UploadCheckPoint.MAGIC
        upload_check_point.upload_file_path = upload_file_request.get_upload_file_path()
        upload_check_point.object_key = upload_file_request.get_object_key()
        upload_check_point.upload_file_info = FileInfo.get_file_info(upload_file_request.get_upload_file_path())
        upload_check_point.upload_parts = MultipartUploadOperation._split_file(
            upload_check_point.upload_file_info.size, upload_file_request.get_part_size())
        upload_check_point.upload_id = self._initiate_multipart(upload_file_request)

    @staticmethod
    def _split_file(file_size, part_size):
        parts = []
        if part_size < MIN_SIZE_PER_PART:
            part_size = MIN_SIZE_PER_PART
        if part_size > MAX_SIZE_PER_PART:
            part_size = MAX_SIZE_PER_PART
        if file_size >= part_size * MAX_PARTS:
            raise AmazonClientException('file is too large.')

        part_num = file_size//part_size
        i = 0
        while True:
            if i == part_num:
                break
            part = UploadPart()
            part.number = i + 1
            part.offset = i * part_size
            part.size = part_size
            part.completed = False
            parts.append(part)
            i = i + 1
        if file_size % part_size > 0:
            part = UploadPart()
            part.number = len(parts) + 1
            part.offset = len(parts) * part_size
            part.size = file_size % part_size
            part.completed = False
            parts.append(part)
        return parts

    def _initiate_multipart(self, upload_file_request):
        try:
            object_metadata = upload_file_request.get_object_metadata()
            gmt_time = get_gmt_time()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, r'application/x-www-form-urlencoded')
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(upload_file_request.get_object_key())
            request_path = "/%s/%s?uploads" % (upload_file_request.get_bucket_name(), object_key)
            authorization = get_authorization(self._passport, request_path, "POST", headers)
            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization

            url = r"http://%s%s" % (self._passport.get_obs_host(), request_path)
            response = requests.post(url=url, data={}, headers=headers, verify=False)
            if response.status_code != 200:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
            handler = CommonXmlResponseHandler(["InitiateMultipartUploadResult"], ["Bucket", "Key", "UploadId"])
            result = handler.handle_success(response)
            return result.ele_names.get("UploadId")
        except Exception as e:
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    @staticmethod
    def _remove_file(file_path):
        if os.path.isfile(file_path) and os.path.exists(file_path):
            os.remove(file_path)

    def _abort_upload(self, upload_check_point, upload_file_request):
        try:
            object_metadata = ObjectMetadata()
            gmt_time = get_gmt_time()
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, APPLICATION_X_WW_FORM_URLENCODED)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(upload_file_request.get_object_key())
            request_path = r"/%s/%s?uploadId=%s" % (upload_file_request.get_bucket_name(), object_key,
                                                    upload_check_point.upload_id)
            authorization = get_authorization(self._passport, request_path, "DELETE", headers)

            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization

            url = r"http://%s%s" % (self._passport.get_obs_host(), request_path)
            response = requests.delete(url=url, data={}, headers=headers, verify=False)
            if response.status_code != 204:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
        except Exception as e:
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    @staticmethod
    def _notify_upload(state, upload_check_point, upload_file_request):
        if not upload_file_request.get_upload_notifier():
            return
        finish_parts = 0
        for upload_part in upload_check_point.upload_parts:
            if upload_part.completed:
                finish_parts = finish_parts + 1
        upload_file_request.get_upload_notifier()(upload_check_point.upload_id, state,
                                                  len(upload_check_point.upload_parts), finish_parts)

    def _upload_multiparts(self, upload_check_point, upload_file_request):
        with open(upload_file_request.get_upload_file_path(), 'rb') as file_object:
            for upload_part in upload_check_point.upload_parts:
                if upload_part.completed:
                    continue
                self._upload_multipart(upload_file_request.get_bucket_name(), upload_file_request.get_object_key(),
                                       upload_check_point.upload_id, upload_part, upload_file_request.get_retry_time(),
                                       file_object, upload_file_request.is_enable_md5())
                upload_check_point.store(upload_file_request.get_checkpoint_file_path())
                self._notify_upload(State.part_uploaded, upload_check_point, upload_file_request)

    def _complete_multipart(self, upload_check_point, upload_file_request):
        upload_part_results = list()
        upload_part_results.append(r'<CompleteMultipartUpload>')
        for upload_part in upload_check_point.upload_parts:
            upload_part_results.append(r'<Part>')
            upload_part_results.append(r'<PartNumber>%s</PartNumber>' % upload_part.number)
            upload_part_results.append(r'<ETag>%s</ETag>' % upload_part.e_tag)
            upload_part_results.append(r'</Part>')
        upload_part_results.append(r'</CompleteMultipartUpload>')

        self._complete_multipart_exec(upload_check_point, upload_file_request, ''.join(upload_part_results))
        self._remove_file(upload_file_request.get_checkpoint_file_path())
        MultipartUploadOperation._notify_upload(State.completed, upload_check_point, upload_file_request)

    def _complete_multipart_exec(self, upload_check_point, upload_file_request, result):
        try:
            gmt_time = get_gmt_time()
            object_metadata = ObjectMetadata()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, APPLICATION_OCTET_STREAM)
            object_metadata.add_metadata(HTTP_CONTENT_LENGTH, str(len(result)))
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(upload_file_request.get_object_key())
            request_path = r"/%s/%s?uploadId=%s" % (upload_file_request.get_bucket_name(), object_key,
                                                    upload_check_point.upload_id)
            authorization = get_authorization(self._passport, request_path, "POST", headers)

            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization
            url = r"http://%s%s" % (self._passport.get_obs_host(), request_path)
            response = requests.post(url=url, data=bytes(result, encoding=DEFAULT_ENCODING), headers=headers,
                                     verify=False)
            if response.status_code != 200:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
            handler = CommonXmlResponseHandler(["CompleteMultipartUploadResult", "Error"],
                                               ["Location", "Bucket", "Key",  "Etag", "Message"])
            result = handler.handle_success(response)
            if "CompleteMultipartUploadResult" != result.root_ele_name:
                raise AmazonClientException(500, result.ele_names.get("Message"))
        except Exception as e:
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)

    def _upload_multipart(self, bucket_name, object_key, upload_id, upload_part, retry_time, file_object, enable_md5):
        retried = -1
        last_exception = None
        part_stream = file_object.read(upload_part.size)
        # part_stream = CustomFileInputStream(file_object, upload_part.offset, upload_part.size)
        object_metadata = ObjectMetadata()
        object_metadata.add_metadata(HTTP_CONTENT_LENGTH, str(upload_part.size))
        if enable_md5:
            md5 = base64_md5(part_stream)
            object_metadata.add_metadata(HTTP_CONTENT_MD5, md5)
        while retried < retry_time:
            try:
                e_tag = self._upload_object_part(bucket_name, object_key, upload_id, part_stream,
                                                 upload_part.number, upload_part.size, object_metadata)
                upload_part.e_tag = trim_tag(e_tag)
                upload_part.completed = True
            except Exception as e:
                # part_stream.reset()
                last_exception = e
            finally:
                retried = retried + 1

        if not upload_part.completed:
            raise last_exception

    def _upload_object_part(self, bucket_name, object_key, upload_id, part_stream, part_number, part_size,
                            object_metadata):
        try:
            gmt_time = get_gmt_time()
            object_metadata.add_metadata(HTTP_CONTENT_TYPE, APPLICATION_X_WW_FORM_URLENCODED)
            object_metadata.add_metadata(HTTP_DATE, gmt_time)
            object_metadata.add_metadata(HTTP_HOST, self._passport.get_obs_host())
            headers = get_request_headers(object_metadata)

            object_key = quote_plus(object_key)
            request_path = r"/%s/%s?partNumber=%d&uploadId=%s" % (bucket_name, object_key, part_number, upload_id)
            authorization = get_authorization(self._passport, request_path, "PUT", headers)

            replace_header_date_to_x_amz_date(headers)
            headers[HTTP_AUTHORIZATION] = authorization
            headers[HTTP_CONTENT_LENGTH] = str(part_size)

            url = r"http://%s%s" % (self._passport.get_obs_host(), request_path)
            response = requests.put(url=url, data=part_stream, headers=headers, verify=False)
            if response.status_code != 200:
                raise AmazonClientException('status code: %s\n text:%s' % (response.status_code, response.text))
            return response.headers[HTTP_E_TAG]
        except Exception as e:
            if isinstance(e, AmazonClientException):
                raise e
            raise AmazonClientException(e)


class CustomFileInputStream(BufferedReader):

    def __init__(self, file_object, offset, length):
        super(CustomFileInputStream, self).__init__(file_object)
        self._file_object = file_object
        self._offset = offset
        self._length = length
        self._file_object.seek(self._offset, os.SEEK_SET)
        self._read_bytes = 0

    def read(self, size=-1):
        remains = self._length - self._read_bytes
        if remains <= 0:
            return -1
        if remains < size or size == -1:
            size = remains
        self._read_bytes += size
        return self._file_object.read(size)

    def reset(self):
        self._file_object.seek(self._offset, os.SEEK_SET)
        self._read_bytes = 0

    def get_read_bytes(self):
        return self._read_bytes

    def get_file_object(self):
        return self._file_object


class UploadCheckPoint(object):

    MAGIC = "f69f21c15c897d3a21121b44956aa517"

    def __init__(self):
        self.magic = None
        self.md5 = None
        self.upload_file_path = None
        self.upload_file_info = None
        self.object_key = None
        self.upload_id = None
        self.upload_parts = []

    def load(self, checkpoint_file_path):
        with open(checkpoint_file_path, 'rb') as file_object:
            ucp = pickle.load(file_object)
            self._assign(ucp)

    def store(self, checkpoint_file_path):
        self.md5 = self.hash_code()
        with open(checkpoint_file_path, 'wb') as file_object:
            pickle.dump(self, file_object)

    def _assign(self, ucp):
        self.md5 = ucp.md5
        self.magic = ucp.magic
        self.upload_file_path = ucp.upload_file_path
        self.upload_file_info = ucp.upload_file_info
        self.object_key = ucp.object_key
        self.upload_id = ucp.upload_id
        self.upload_parts = ucp.upload_parts

    def is_valid(self, upload_file_path):
        if self.magic != UploadCheckPoint.MAGIC or self.md5 != self.hash_code():
            return False
        if not os.path.exists(upload_file_path):
            return False
        if self.upload_file_path != upload_file_path \
            or self.upload_file_info.size != os.path.getsize(upload_file_path) \
            or self.upload_file_info.last_modified != os.path.getmtime(upload_file_path):
            return False
        return True

    def hash_code(self):
        data = pickle.dumps(self)
        return hashlib.new("md5", data=data).hexdigest()


class FileInfo(object):

    def __init__(self):
        self.size = None
        self.last_modified = None

    @staticmethod
    def get_file_info(upload_file_path):
        file_info = FileInfo()
        file_info.size = os.path.getsize(upload_file_path)
        file_info.last_modified = os.path.getmtime(upload_file_path)
        return file_info

    def get_string(self):
        return str(self.size) + str(self.last_modified)


class UploadPart(object):

    def __init__(self):
        self.number = None
        self.offset = None
        self.size = None
        self.e_tag = None
        self.completed = None

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: liaoli

import os
import re
import traceback
import logging
import random
import math

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(filename)s-%(lineno)d-%(funcName)s -> %(message)s',
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class M3u8Segment():
    def __init__(self, extinf=None, ts=None, discontinuity=None):
        self.extinf = extinf
        self.ts = ts  #"test-0-1610680398867.ts"
        self.discontinuity = discontinuity

    def get_timems(self):
        info = self.ts.split("-")[-1]
        timestr = info.split(".")[0]
        return int(timestr)

    def get_duration(self):
        info = self.extinf.split(",")[0]
        duration = info.split(":")[-1]
        return float(duration)


class M3U8Parser(object):
    def __init__(self, path, start=-1, end=-1, parse_path=None):
        self.params_path = path
        self.params_start = start
        self.params_end = end

        self.parse_path = parse_path
        self.parse_path_ts = None
        self.parse_seg_list = list()  # M3u8Segment
        self.parse_header = "#EXTM3U"
        self.parse_version = "#EXT-X-VERSION:3"
        self.parse_sequence = "#EXT-X-MEDIA-SEQUENCE:0"
        self.parse_target_duration = "#EXT-X-TARGETDURATION:45"

        if os.path.exists(self.params_path):
            self._parse_m3u8_file()

    def remove_all(self):
        """
        1、删除M3u8、中间解析文件、TS文件
        2、删除M3U8所在当前目录
        """
        # path_list = self.params_path.split("/")[:-1]
        index = self.params_path.rfind("/")
        root_path = self.params_path[:index]
        for seg in self.parse_seg_list:
            path = os.path.join(root_path, seg.ts)
            if os.path.exists(path):
                # pass
                # TEST don't remove ts file
                os.remove(path)
        if os.path.exists(self.params_path):
            os.remove(self.params_path)
        if os.path.exists(self.parse_path):
            os.remove(self.parse_path)
        if os.path.exists(self.parse_path_ts):
            os.remove(self.parse_path_ts)
        try:
            os.rmdir(root_path)
        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))

    def get_ts_list(self):
        """
        返回TS列表
        """
        return [seg.ts for seg in self.parse_seg_list]

    def is_discontinuity(self):
        """
        返回录制列表是否连续
        """
        for seg in self.parse_seg_list:
            if seg.discontinuity and self.parse_seg_list.index(seg) != 0:
                return True
        return False

    def get_m3u8_path(self):
        """
        返回M3U8列表(新建的/原始的)
        """
        if self.parse_path is None:
            return self.params_path
        else:
            return self.parse_path

    def get_flist_path(self):
        """
        返回切片文件列表
        """
        return self.parse_path_ts

    def get_start_timems(self):
        """
        返回开始M3U8列表开始录制时间 ms
        """
        if len(self.parse_seg_list) > 0:
            seg = self.parse_seg_list[0]
            return seg.get_timems()

    def get_end_timems(self):
        """
        返回开始M3U8列表结束录制时间 ms
        """
        if len(self.parse_seg_list) > 0:
            seg = self.parse_seg_list[-1]
            return int(seg.get_timems() + seg.get_duration() * 1000)

    def get_duration_timems(self):
        """
        返回开始M3U8列表 录制总时长 s
        """
        duration = 0.0
        for seg in self.parse_seg_list:
            duration += seg.get_duration()
        return int(duration * 1000)

    def _create_parse_path(self):
        with open(self.parse_path, 'w') as f:
            f.writelines(['#EXTM3U\n', self.parse_version + '\n', self.parse_target_duration + '\n'])  # self.parse_sequence+'\n',

            for seg in self.parse_seg_list:
                if seg.discontinuity == True:
                    f.writelines(['#EXT-X-DISCONTINUITY\n', seg.extinf + '\n', seg.ts + '\n'])
                else:
                    f.writelines([seg.extinf + '\n', seg.ts + '\n'])

            f.writelines(['#EXT-X-ENDLIST\n'])

    def _create_ts_path(self):
        with open(self.parse_path_ts, 'w') as f:
            for seg in self.parse_seg_list:
                f.writelines(['file ' + seg.ts + '\n'])

    def _parse_m3u8_file(self):
        try:
            with open(self.params_path, 'r+') as f:
                f.seek(0)
                lines = f.readlines()

                skip_flag = False
                extinf = ts = discontinuity = None
                for line in lines:
                    line = line.strip()
                    if line.startswith('#EXTM3U'):
                        continue
                    elif line.startswith('#EXT-X-VERSION'):
                        self.parse_version = line
                        continue
                    elif line.startswith('#EXT-X-MEDIA-SEQUENCE'):
                        self.parse_sequence = line
                        continue
                    elif line.startswith('#EXT-X-TARGETDURATION'):
                        self.parse_target_duration = line
                        continue
                    else:
                        if line.startswith('#EXT-X-DISCONTINUITY'):
                            discontinuity = True
                            continue
                        elif line.startswith('#EXTINF'):
                            extinf = line
                            continue
                        elif line.endswith('.ts'):
                            ts = line

                            if extinf is not None:
                                segment = M3u8Segment(extinf, ts, discontinuity)
                                if (self.params_start <= 0 and self.params_end <= 0):
                                    self.parse_seg_list.append(segment)
                                elif self.params_start > 0 and self.params_end > 0:
                                    if (segment.get_timems() > self.params_start or segment.get_timems() + int(segment.get_duration() * 1000) >
                                            self.params_start) and segment.get_timems() < self.params_end:
                                        self.parse_seg_list.append(segment)
                                    else:
                                        skip_flag = True
                                else:
                                    if self.params_start > 0 and (segment.get_timems() > self.params_start
                                                                  or segment.get_timems() + int(segment.get_duration() * 1000) > self.params_start):
                                        self.parse_seg_list.append(segment)
                                    elif self.params_end > 0 and segment.get_timems() < self.params_end:
                                        self.parse_seg_list.append(segment)
                                    else:
                                        skip_flag = True
                            else:
                                logger.error("m3u8 format error:{}".format(ts))
                            extinf = ts = discontinuity = None
                            continue
            if self.parse_path is None:
                self.parse_path = self.params_path.replace(".m3u8", "_parse.m3u8")
            if self.parse_path_ts is None:
                self.parse_path_ts = self.params_path.replace(".m3u8", "_ts.txt")
            self._create_parse_path()
            self._create_ts_path()

        except Exception as e:
            logger.error("exception:{} trace:{}".format(e, traceback.format_exc()))


if __name__ == "__main__":
    m3u8_parse = M3U8Parser("/dev/shm/data/livehls/www.test.com/live/test/test.m3u8")
    logger.info("get_m3u8_path:{}".format(m3u8_parse.get_m3u8_path()))
    logger.info("get_ts_list:{}".format(m3u8_parse.get_ts_list()))
    logger.info("get_start_timems:{}".format(m3u8_parse.get_start_timems()))
    logger.info("get_end_timems:{}".format(m3u8_parse.get_end_timems()))
    logger.info("len:{}".format(m3u8_parse.get_end_timems() - m3u8_parse.get_start_timems()))
    logger.info("get_duration_timems:{}".format(m3u8_parse.get_duration_timems()))

    m3u8_parse = M3U8Parser("/dev/shm/data/livehls/www.test.com/live/test/test.m3u8", start=1610956250000)
    logger.info("get_m3u8_path:{}".format(m3u8_parse.get_m3u8_path()))
    logger.info("get_ts_list:{}".format(m3u8_parse.get_ts_list()))
    logger.info("get_start_timems:{}".format(m3u8_parse.get_start_timems()))
    logger.info("get_end_timems:{}".format(m3u8_parse.get_end_timems()))
    logger.info("len:{}".format(m3u8_parse.get_end_timems() - m3u8_parse.get_start_timems()))
    logger.info("get_duration_timems:{}".format(m3u8_parse.get_duration_timems()))

    # m3u8_parse = M3U8Parser("/dev/shm/data/livehls/www.test.com/live/test/test.m3u8", start=1610956250000, end=1610956290000)
    # logger.info("get_m3u8_path:{}".format(m3u8_parse.get_m3u8_path()))
    # logger.info("get_ts_list:{}".format(m3u8_parse.get_ts_list()))
    # logger.info("get_start_timems:{}".format(m3u8_parse.get_start_timems()))
    # logger.info("get_end_timems:{}".format(m3u8_parse.get_end_timems()))
    # logger.info("len:{}".format(m3u8_parse.get_end_timems() - m3u8_parse.get_start_timems()))
    # logger.info("get_duration_timems:{}".format(m3u8_parse.get_duration_timems()))

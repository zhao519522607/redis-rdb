#!/usr/bin/env python
# -*- coding=utf-8 -*-

from gevent import monkey; monkey.patch_all()
import os
import sys
from rdbtools import RdbParser,RdbCallback,JSONCallback
import json
import pickle
import re
import logging
from logger import Logger
import traceback

sys.path.append("../..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ParseCallback(RdbCallback):
    def __init__(self,logger=None):
        self._logger = logger
        self._file_handle = None

    def close_file(self):
        try:
            if self._file_handle:
                self._file_handle.close()
        except:
            self._logger.error(traceback.format_exc())

    def write_file(self,file_name,info):
        try:
            target_file_name = "/home/redis-stress/" + os.path.basename(file_name)

            if self._file_handle is None:
                self._file_handle = open(target_file_name,'wb')

            if self._file_handle:
                pickle.dump(info,self._file_handle)

        except:
            self._logger.error(traceback.format_exc())

    def load_file(self,file_name):
        try:
            pass
        except:
            self._logger.error(traceback.format_exc())

    def hset(self,key,field,value):
        try:
                json_dict = dict()
                info = "%s.%s = %s"%(str(key), str(field), str(value))
                json_dict[str(key)] = info

                self.write_file('hash.txt',json_dict)
                self._logger.info(info)
        except:
            self._logger.error(traceback.format_exc())

def __main__():

    #初始化日志服务
    cur_path = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(cur_path, 'log')

    if not Logger.init(log_path):
        return False

    #callback = JSONCallback(sys.stdout)

    test_callbak = ParseCallback(Logger.logger)

    filters = dict()
    filters['types'] = list()
    filters['types'].append('hash')

    parser = RdbParser(test_callbak, filters=filters)
    dump_file='/data/temp/dump.rdb'
    parser.parse(dump_file)

#rdbtools 用来分析rdb文件
if __name__ == '__main__':
    __main__()

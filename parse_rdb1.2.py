#!/usr/bin/env python
# -*- coding=utf-8 -*-

from gevent import monkey; monkey.patch_all()
import sys
import os
from rdbtools import RdbParser,RdbCallback,JSONCallback
import json
import pickle
import re
import logging
import gevent
from gevent.queue import Queue
from gevent.pool import Group
from gevent import getcurrent
from logger import Logger
import traceback
import redis
import datetime
import time
from multiprocessing import Process,cpu_count
from optparse import OptionParser
from socket import error as SocketError
import errno
from threading import Timer
import sched
from gevent.coros import BoundedSemaphore
import random

sys.path.append("../..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

#REDIS_HOST = '1.1.1.1'
REDIS_HOST = '1.1.1.1'
REDIS_PORT = 000

GLOBAL_HASH = dict() #用来模拟hash
GLOBAL_LIST = list() #用来模拟list
GLOBAL_SET  = set()  #用来模拟set

GLOBAL_HASH_VALUE = dict() #用来模拟hash

GLOBAL_RECOERS_NUMS = 1000

SEC_SEND_RECORD = 1000

TV_MSEC = 1000

class StressCount(object):
    def __init__(self):
        self._success = 0
        self._fail    = 0
        self._avg_rsp_time = 0
        #self._sem  = BoundedSemaphore(2)

    def set_stress_count(self,flag,avg_rsp_time):

        #self._sem.acquire()
        if flag:
            self._success +=1
        else:
            self._fail    +=1

        self._avg_rsp_time += avg_rsp_time
        #self._sem.release()

    def reset(self):
        #self._sem.acquire()
        self._success = 0
        self._fail    = 0
        self._avg_rsp_time = 0
        #self._sem.release()

class Tools(object):
    @classmethod
    def get_now_string(cls):
        val = datetime.datetime.now()
        now_string = val.strftime('%Y-%m-%d %H:%M:%S')
        return now_string

class StressTest(object):
    def __init__(self,logger=None,redis_manage_obj=None):
        self._logger = logger
        self._redis_manange_obj =  redis_manage_obj
        self._group = None
        self._pid   = None
        self._schedule = None
        self._stress_count = None

    def init(self):
        try:
            if self._redis_manange_obj:
               self._redis_manange_obj.init_redis_pool()
               self._group = Group()
               self._pid   = os.getpid()
               self._stress_count = StressCount()
               self._schedule = sched.scheduler(time.time, time.sleep)
        except:
            self._logger.error(traceback.format_exc())

    def schedule(self,msg):
        try:
            while True:
                self._schedule.enter(1,0,self.sectond_print,("test1",))
                self._schedule.run()
        except:
            self._logger.error(traceback.format_exc())

    def sectond_print(self,stress_count_obj,off_time):
        try:
            if stress_count_obj:
                total_send   = stress_count_obj._success + stress_count_obj._fail
                if total_send > 0:
                    avg_rsp_time = stress_count_obj._avg_rsp_time/total_send
                    info = "process pid:%d,total:%d,success:%d,fail:%d,avg_rsp_time(ms):%d,send_time:%d"%(self._pid,total_send,stress_count_obj._success,stress_count_obj._fail,avg_rsp_time,off_time)
                    self._logger.info(info)
        except:
            self._logger.error(traceback.format_exc())

    def process_set(self,data_list):
        try:
            redis_conn = self._redis_manange_obj.get_redis_obj()
            while True:
                try:
                    if len(data_list):
                        for item in random.sample(data_list,len(data_list)):
                             if not redis_conn.exists(item):
                                  value_list = GLOBAL_HASH_VALUE[item].split('=')
                                  redis_conn.hset(item,value_list[0],value_list[1])
                             gevent.sleep(0.02)
                except:
                    self._logger.error(traceback.format_exc())
        except:
            self._logger.error(traceback.format_exc())

    def process_del(self,data_list):
        try:
            redis_conn = self._redis_manange_obj.get_redis_obj()
            while True:
                try:
                    if len(data_list):
                        for item in random.sample(data_list,len(data_list)):
                            if not redis_conn.exists(item):
                                continue
                            field = GLOBAL_HASH[item].split('=')[0]
                            redis_conn.hdel(item,field)
                            gevent.sleep(0.01)
                except:
                    self._logger.error(traceback.format_exc())
        except:
            self._logger.error(traceback.format_exc())

    def process_data(self,data_list,send_data):
        try:
            redis_conn = self._redis_manange_obj.get_redis_obj()

            current_send = 0
            send_time    = 0.00

            stress_count_obj = StressCount()

            while True:
                try:
                    if len(data_list):
                        if redis_conn:
                            break_flag = False
                            record_time   = time.time()
                            while True:
                                if break_flag:
                                    break
                                for item in data_list:
                                    if current_send < send_data:
                                        begin_time = time.time()
                                        field  = GLOBAL_HASH[item].split('=')[0]
                                        result = redis_conn.hget(item,field)
                                        end_time = time.time()
                                        off_time = float((end_time - begin_time)) * 1000
                                        current_send +=1

                                        if result:
                                            stress_count_obj.set_stress_count(True,off_time)
                                        else:
                                            stress_count_obj.set_stress_count(False,off_time)
                                    else:
                                        send_time  = float((time.time() - record_time)) * 1000
                                        break_flag = True
                                        break

                            if send_time < TV_MSEC:
                                sec = float(TV_MSEC-send_time)/1000
                                current_send = 0
                                self.sectond_print(stress_count_obj,send_time)
                                stress_count_obj.reset()
                                send_time=0.00
                                gevent.sleep(sec)
                            else:
                                current_send = 0
                                self.sectond_print(stress_count_obj,send_time)
                                stress_count_obj.reset()
                                send_time = 0.00
                                gevent.sleep(1)
                    else:
                        self._logger.error("error")
                except:
                    self._logger.error(traceback.format_exc())

        except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    self._logger.error("socket connect reset!!!")
                    self.process_data(data_list)
        except:
            self._logger.error(traceback.format_exc())

    def slice_data_list(self):
        try:
            n_group  = len(GLOBAL_LIST)/GLOBAL_RECOERS_NUMS
            n_offset = len(GLOBAL_LIST)%GLOBAL_RECOERS_NUMS

            if n_offset > 0:
                n_groups = n_group + 1
            else:
                n_groups = n_group

            sec_send = SEC_SEND_RECORD/n_groups

            info = "gevent thread:%d,one gevent thread,sec send:%d"%(n_groups,sec_send)
            self._logger.info(info)

            for i in range(n_groups):
                if i == n_groups - 1:
                    begin = i * GLOBAL_RECOERS_NUMS
                    end   = i * GLOBAL_RECOERS_NUMS + n_offset
                else:
                    begin = i * GLOBAL_RECOERS_NUMS
                    end   = i * GLOBAL_RECOERS_NUMS + GLOBAL_RECOERS_NUMS

                index_list = GLOBAL_LIST[begin:end]
                g = gevent.spawn(self.process_data,index_list,sec_send)
                self._group.add(g)
                g_del = gevent.spawn(self.process_del,index_list)
                self._group.add(g_del)
                g_set = gevent.spawn(self.process_set,index_list)
                self._group.add(g_set)

            self._group.join()
        except:
            self._logger.error(traceback.format_exc())

    def hash_stress_test(self):
        try:
            if self._redis_manange_obj:
                redis_conn = self._redis_manange_obj.get_redis_obj()

                if redis_conn:
                    for item in GLOBAL_HASH:
                        if not redis_conn.exists(item):
                            #self._logger.error(item)
                            continue
                        result = redis_conn.hget(item,GLOBAL_HASH[item])
                        # if result:
                        #     self._logger.info(result)
        except:
            self._logger.error(traceback.format_exc())

    def string_stress_test(self):
        try:
            if self._redis_manange_obj:
                redis_conn = self._redis_manange_obj.get_redis_obj()
                if redis_conn:
                    for item in GLOBAL_SET:
                        redis_conn.get(item)
        except:
            self._logger.error(traceback.format_exc())

    def list_stress_test(self):
        try:
            if self._redis_manange_obj:
                redis_conn = self._redis_manange_obj.get_redis_obj()
        except:
            self._logger.error(traceback.format_exc())

class RedisManage(object):
    def __init__(self,logger=None):
        self._logger         = logger
        self._redis_pool_obj = None

    def init_redis_pool(self):
        try:
            self._redis_pool_obj = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0)
        except:
            self._logger.error(traceback.format_exc())

    def get_redis_obj(self):
        try:
            redis_conn_obj = None
            if self._redis_pool_obj:
                redis_conn_obj = redis.Redis(connection_pool=self._redis_pool_obj)
            return redis_conn_obj
        except:
            self._logger.error(traceback.format_exc())

class ParseCallback(RdbCallback):
    def __init__(self,logger=None,redis_manage_obj=None,file_name=None):
        self._logger      = logger
        self._file_handle = None
        self._redis_manange_obj = redis_manage_obj
        self._file_name   = file_name

    def close_file(self):
        try:
            if self._file_handle:
                self._file_handle.close()
        except:
            self._logger.error(traceback.format_exc())

    def write_file(self,info):
        try:
            if self._file_handle is None:
                self._file_handle = open(self._file_name,'wb')

            if self._file_handle:
                self._file_handle.write(info)

        except:
            self._logger.error(traceback.format_exc())

    def push_rdb_hash(self,info_dict):
        try:
            for key,value in info_dict.items():
                value_str = value.split('=')[0]
                GLOBAL_HASH[key] = value
                # GLOBAL_HASH_VALUE[key] = value
                # GLOBAL_LIST.append(key)
        except:
            self._logger.error(traceback.format_exc())

    def push_test_data_to_redis(self):
        try:
            if GLOBAL_HASH:
                redis_conn = self._redis_manange_obj.get_redis_obj()
                if redis_conn:
                    for key in GLOBAL_HASH:
                        value =  GLOBAL_HASH[key].split('=')
                        info = "key:%s,value:%s"%(key,GLOBAL_HASH[key])
                        self._logger.info(info)
                        result = redis_conn.hset(key,value[0],value[1])
                        if result !=0:
                            self._logger.info(result)
        except:
            self._logger.error(traceback.format_exc())

    def load_file(self):
        try:
            if os.path.exists(self._file_name):
                 file_handle = open(self._file_name, "rb")
                 if file_handle:
                    file_handle.seek(0,2)
                    being_time = Tools.get_now_string()
                    self._logger.info("begin load %s,time:%s"%(self._file_name,being_time))
                    for line in file_handle.readlines():
                        info_dict = json.loads(line)
                        if info_dict:
                            self._logger.error(line)
                            self.push_rdb_hash(info_dict)
                    end_time = Tools.get_now_string()
                    self._logger.info("end load time:%s"%(end_time))
                    self.close_file()
        except:
            self._logger.error(traceback.format_exc())

    def hset(self,key,field,value):
        try:
                json_dict = dict()
                info = "%s=%s"%(str(field),str(value))
                json_dict[str(key)] = info

                json_str  = json.dumps(json_dict) + '\n'

                self.write_file(json_str)
                self._logger.info(info)
        except:
            self._logger.error(traceback.format_exc())

def func(log_obj,redis_manage_obj):

    stress_test_obj = StressTest(log_obj,redis_manage_obj)
    stress_test_obj.init()
    stress_test_obj.slice_data_list()
    #stress_test_obj.schedule()

def __main__():

    total_cores = cpu_count()

    #初始化日志服务
    cur_path   = os.path.dirname(os.path.abspath(__file__))
    log_path   = os.path.join(cur_path, 'log')
    bin_path   = os.path.join(cur_path, "bin")

    pidfile = os.path.join(bin_path, "server.pid")
    cur_pid = os.getpid()

    exec_command  = "echo %d > %s"%(cur_pid,pidfile)
    os.system(exec_command)

    if not Logger.init(log_path):
        return False

    data_file = os.path.join(cur_path,"hash.txt")
    #
    # if not os.path.exists(data_file):
    #     Logger.logger.error("file not exists!!")
    #     return

    redis_manage_obj = RedisManage(Logger.logger)

    redis_manage_obj.init_redis_pool()
    test_callbak = ParseCallback(Logger.logger,redis_manage_obj,data_file)
    test_callbak.load_file()
    test_callbak.push_test_data_to_redis()

    # jobs = list()
    # for x in xrange(total_cores):
    #     p = Process(target = func, args=(Logger.logger,redis_manage_obj))
    #     p.start()
    #     jobs.append(p)
    #
    # for job in jobs:
    #     job.join()

    filters = dict()
    filters['types'] = list()
    filters['types'].append('hash')
    parser = RdbParser(test_callbak, filters=filters)
    dump_file='/data/temp/dump.rdb'
    parser.parse(dump_file)

#rdbtools 用来分析rdb文件
if __name__ == '__main__':
    __main__()

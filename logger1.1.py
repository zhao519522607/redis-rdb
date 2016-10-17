#!/usr/bin/env python
# -*- coding=utf-8 -*-

from gevent import monkey; monkey.patch_all()
import os
import logging
import traceback
from cloghandler import ConcurrentRotatingFileHandler

class Logger(object):
    # 几个日志函数
    debug = None
    info = None
    error = None

    # 几个日志类
    logger = None

    # 初始化日志模块
    @classmethod
    def init(cls, log_path):
        """
        :type log_path: str
        :return: bool
        """
        try:
            # 如果目录不存在，创建目录
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            print 'log_path: ', log_path

            # 日志格式
            formatter = logging.Formatter('[%(asctime)s]--[%(process)d]--[%(levelname)s]--%(message)s')

            # 通用日志
            log_file = os.path.join(log_path, 'logger.log')
            logger_fh = ConcurrentRotatingFileHandler(log_file, "a", 10 * 1024 * 1024, 300)
            logger_fh.setLevel(logging.DEBUG)
            logger_fh.setFormatter(formatter)
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logger_fh)
            logger.info('%s init successful!' % log_file)
            cls.logger = logger

            # DEBUG日志
            # debug_file = os.path.join(log_path, 'debug.log')
            # debug_fh = ConcurrentRotatingFileHandler(debug_file, "a", 10 * 1024 * 1024, 100)
            # debug_fh.setLevel(logging.DEBUG)
            # debug_fh.setFormatter(formatter)
            # debug_logger = logging.getLogger()
            # debug_logger.setLevel(logging.DEBUG)
            # debug_logger.addHandler(debug_fh)
            # debug_logger.info('%s init successful!' % log_file)
            cls.debug = logger.debug

            # INFO日志
            # info_file = os.path.join(log_path, 'info.log')
            # info_fh = ConcurrentRotatingFileHandler(info_file, "a", 10 * 1024 * 1024, 100)
            # info_fh.setLevel(logging.DEBUG)
            # info_fh.setFormatter(formatter)
            # info_logger = logging.getLogger()
            # info_logger.setLevel(logging.DEBUG)
            # info_logger.addHandler(info_fh)
            # info_logger.info('%s init successful!' % log_file)
            cls.info = logger.info

            # ERROR日志
            # error_file = os.path.join(log_path, 'error.log')
            # error_fh = ConcurrentRotatingFileHandler(error_file, "a", 10 * 1024 * 1024, 100)
            # error_fh.setLevel(logging.DEBUG)
            # error_fh.setFormatter(formatter)
            # error_logger = logging.getLogger()
            # error_logger.setLevel(logging.DEBUG)
            # error_logger.addHandler(error_fh)
            # error_logger.error('%s init successful!' % log_file)
            cls.error = logger.error

            # 返回三个日志类
            return True
        except:
            print traceback.format_exc()
            return False

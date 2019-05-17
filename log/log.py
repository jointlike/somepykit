#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
名称：
日期：2018/8/24 10:45
@author: Tim.Wells

服务概述：

"""
from setting.conf import *
from kafka import KafkaProducer
import logging.handlers
import os
import json
import random


# 日志配置
class MyLogging(object):
    def __init__(self):
        logging.basicConfig(level=logging.INFO,  # 最低级别要求
                            format='%(asctime)s %(threadName)s %(filename)s '
                                   '[line:%(lineno)d] %(levelname)s: %(message)s')

    def get_file_logging(self, file_name):
        base_fmt = """{"head": {"app_name": "%(app_name)s","app_version": "%(app_version)s", "host_name": "%(host_name)s",
                   "ip": "%(ip_address)s","os": "%(os_name)s","dev": "%(environment)s","""

        dys_fmt = """"file_name": "%(pathname)s","file_line": %(lineno)d,"funcname": "%(funcName)s",
                   "thread_id": %(thread)d,"thread_name": "%(threadName)s","log_date": %(created)f,
                   "log_level": "%(levelname)s"},"content": "%(message)s"}"""

        basic_info = {'app_name': APP_NAME, 'app_version': APP_VERSION, 'host_name': HOST_NAME,
                      'ip_address': IP_ADDR, 'os_name': OS_NAME, 'environment': ENVIRONMENT}

        form = base_fmt % basic_info + dys_fmt
        fname = os.path.join(LOG_DIR, file_name)
        file_handle = logging.handlers.TimedRotatingFileHandler(fname, when='h', interval=1)
        file_handle.suffix = '%Y%m%d%H%M%S.log'
        file_handle.setLevel(logging.INFO)
        file_handle.setFormatter(logging.Formatter(form))
        logging.getLogger().addHandler(file_handle)
        return logging

    @staticmethod
    def get_socket_logging(name):
        logger = logging.getLogger(name)
        kh = KafkaLoggingHandler()
        kh.setFormatter(DictFormatter())
        logger.addHandler(kh)
        return logger

    def get_logger(self, file_name=None):
        if LOG_TO_KAFKA:
            return self.get_socket_logging(file_name)
        return self.get_file_logging(file_name)


class KafkaLoggingHandler(logging.Handler):
    def __init__(self):
        self.level = logging.INFO
        logging.Handler.__init__(self)
        self.producer = KafkaProducer(bootstrap_servers=LOG_KAFKA_HOST,
                                      value_serializer=lambda m: json.dumps(m).encode())

    def emit(self, record):
        try:
            buf = self.formatter.format(record)
            self.producer.send(KAFKA_TOPIC, buf)
        except Exception as e:
            print('kafka logging error:', e)
            self.handleError(record)

    def close(self):
        self.producer.close()
        logging.Handler.close(self)


class DictFormatter(object):
    def format(self, record):
        msg = record.getMessage()
        if 'call_id' in msg and 'caller' in msg:
            a, b, c, d = msg.split('--')
            desc, call_id, caller, called = a, b.split(':')[-1], c.split(':')[-1], d.split(':')[-1]
        else:
            desc, call_id, caller, called = msg, None, None, None
        fmt = {"head":
                   {"app_name": APP_NAME,
                    "app_version": APP_VERSION,
                    "host_name": HOST_NAME,
                    "ip": IP_ADDR,
                    "os": OS_NAME,
                    "dev": ENVIRONMENT,
                    "file_name": record.filename,
                    "file_line": record.lineno,
                    "funcname": record.funcName,
                    "thread_id": record.thread,
                    "thread_name": record.threadName,
                    "log_date": int(record.created),
                    "log_level": record.levelname,
                    "random": self.ra()
                    },
               "content":
                   {'desc': desc,
                    'call_id': call_id,
                    'caller': caller,
                    'called': called
                    }
               }
        return fmt

    @staticmethod
    def ra():
        x = str(random.randint(1, 9))
        for i in range(15):
            x += str(random.randint(0, 9))
        return x

'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-06-28 12:44:26
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
import psycopg2
import pymysql
from datetime import date, timedelta, datetime
import re
import sys
import pandas as pd
from pandas import DataFrame
from params import *
from .dbbase import DBBase

import logging
import logging.handlers


#1.创建logger
rlogger = logging.getLogger(name='db_api')
rlogger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './log/db.log'
fh = logging.handlers.TimedRotatingFileHandler(
    logfile, when='D', interval=1, backupCount=100, encoding='utf-8')
fh.setLevel(logging.ERROR)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
rlogger.addHandler(fh)
rlogger.addHandler(ch)


class DBRedshift(DBBase):
    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = '5439'
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            rlogger.error(e)


class DBMysql(DBBase):
    def __init__(self, host=None, user=None, password=None, db=None):
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.db = db
        self.conn = None

    def connect(self):
        try:
            self.conn = pymysql.connect(
                db=self.db, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            rlogger.error(e)

    def get_table_info(self, tablename, column, func='max'):
        if func not in ['min', 'max', 'sum', 'count']:
            raise "func only support 'min', 'max', 'sum', 'count'"

        if not self.conn:
            self.connect()
            
        sql = self.sql_for_select(tablename=tablename, columns=[column])
        result = self.sql_execute(sql)
        if not result:
            result = 0
        else:
            if func == 'min':
                result = min(result)[0]
            elif func == 'max':
                result = max(result)[0]
            elif func == 'sum':
                result = sum(result)[0]
            elif func == 'count':
                result = len(result)[0]
        return result
        

        
            

'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-07-01 12:45:10
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
from .dbbase import DBBase

import logging
from logging import config

config.fileConfig('parselog.conf')
dblogger = logging.getLogger('db')

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
            dblogger.error(e)


class DBMysql(DBBase):
    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.db = database
        self.conn = None

    def connect(self):
        try:
            self.conn = pymysql.connect(
                db=self.db, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            dblogger.error(e)

    def get_table_info(self, tablename, column, func='max'):
        if func not in ['min', 'max', 'sum', 'count']:
            raise "func only support 'min', 'max', 'sum', 'count'"

        if not self.conn:
            self.connect()
            
        sql = self.sql_for_select(tablename=tablename, columns=[column])
        _, result = self.sql_execute(sql)
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
    
    def delete_by_id(self, tablename, id_min=None, id_max=None):
        if not self.conn:
            self.connect()
        if id_min and id_max:
            contion = f'id > {id_min} and id <= {id_max}'
        elif id_min and not id_max:
            contion = f'id > {id_min}'
        else:
            contion = f'id >= 0'
        sql = self.sql_for_delete(tablename, contion=contion)
        self.sql_execute(sql)
        



        
            

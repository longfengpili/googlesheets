'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-06-24 13:52:58
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
import psycopg2
from datetime import date, timedelta, datetime
import re
import sys

import pandas as pd
from pandas import DataFrame

from params import *

import logging

#1.创建logger
rlogger = logging.getLogger(name='redshift')
rlogger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './my.log'
fh = logging.FileHandler(logfile,mode='a')
fh.setLevel(logging.ERROR)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
rlogger.addHandler(fh)
rlogger.addHandler(ch)


class DBRedshift():
    def __init__(self):
        self.database = DATABASE
        self.user = USER
        self.password = PASSWORD
        self.host = HOST
        self.port = '5439'

    def __redshift_connect(self):
        try:
            redshift_connection = psycopg2.connect(
                database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            rlogger.error(e)
        return redshift_connection

    def __check_sql_type(self, sql):
        result = re.match('(\D.*?) ',sql)
        return result.group(1)

    def redshift_execute(self, sql, count=None):
        sql_type = self.__check_sql_type(sql)
        result = f'{sql_type} completed !'
        # rlogger.info(sql_type)
        
        conn = self.__redshift_connect()
        cursor = conn.cursor() 

        if sql_type == 'select':
            if count:
                cursor.execute(sql)
                result = cursor.fetchmany(count)
            else:
                cursor.execute(sql)
                result = cursor.fetchall()
        else:
            try:
                cursor.execute(sql)
                conn.commit() 
            except Exception as e:
                conn.rollback()
                rlogger.error(e)
            
        conn.close()
        return result

    def sql_for_create_table(self, tablename, columns):
        if not isinstance(columns,dict):
            raise 'colums must be a dict ! example:{"column_name":"column_type"}'
        sql = f'''create table if not exists {tablename}
                    ({','.join([k.lower() + ' '+ v for k, v in columns.items()])});'''
         
        sql = re.sub('\s{2,}','\n',sql)
        return sql

    def sql_for_drop_table(self, tablename):
        sql = f'drop table if exists {tablename};'
        return sql

    def sql_for_insert_table(self, tablename, columns, values):
        columns = ','.join(columns.keys())
        values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        sql = f'''insert into {tablename}
                 ({columns})
                 values
                 {values};'''
        sql = re.sub('\s{2,}', '\n', sql)
        return sql
 
        
        
            

'''
@Author: longfengpili
@Date: 2019-07-01 10:11:18
@LastEditTime: 2019-07-01 10:11:18
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python
# -*- coding:utf-8 -*-


import psycopg2
import pymysql
from datetime import date, timedelta, datetime
import re
import sys
from .dbbase import DBBase
import time

import logging
from logging import config
import threading

config.fileConfig('parselog.conf')
dblogger = logging.getLogger('db')

class DBFunction(DBBase):
    '''
    一些共用的内容
    '''
    def __init__(self):
        pass

    def create_table(self, tablename, columns):
        sql = self.sql_for_create(tablename=tablename, columns=columns)
        self.sql_execute(sql)

    def delete_by_id(self, tablename, id_min=None, id_max=None):
        '''
        删除数据通过ID
        '''
        if id_min >= 0 and id_max:
            contion = f'id >= {id_min} and id <= {id_max}'
        elif id_min and not id_max:
            contion = f'id >= {id_min}'
        else:
            contion = f'id >= 0'
        sql = self.sql_for_delete(tablename, contion=contion)
        self.sql_execute(sql)
        dblogger.warning(f'【delete】tablename [{tablename}], contion [{contion}] !')
    
    def get_table_id(self, tablename, column='id', func='max'):
        sql = self.sql_for_column_agg(tablename, column=column, func=func)
        _, result = self.sql_execute(sql)
        result = result[0][0] if result[0][0] else 0 
        return result

    def get_table_count(self, tablename):
        sql = self.sql_for_column_agg(tablename=tablename)
        _, result = self.sql_execute(sql)
        result = result[0][0] if result[0][0] else 0
        return result

class DBRedshift(DBFunction):
    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = '5439'
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def _connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            dblogger.error(e)
            while not self.conn:
                self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
                time.sleep(1)

    def get_conn_instance(self):
        try:
            conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            conn = None
            dblogger.error(e)
            while not conn:
                conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
                time.sleep(1)
        return conn

    def get_table_columns(self, tablename):
        sql = f"""
        select column_name
        from information_schema.columns
        where table_schema = '{tablename.split('.')[0]}'
        and table_name = '{tablename.split('.')[1]}';
        """
        counts, result = self.sql_execute(sql)
        columns = [column[0] for column in result]
        return columns

    def alter_table_columns(self, tablename, columns):
        original_columns = self.get_table_columns(tablename)
        print(f'{tablename} already have {original_columns} !')
        for k, v in columns.items():
            if k not in original_columns:
                sql = f'alter table {tablename} add column {k} {v + "(128)" if v == "varchar" else v};'
                self.sql_execute(sql)
                print(f'add {k} success !')

class DBMysql(DBFunction):
    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def _connect(self):
        try:
            self.conn = pymysql.connect(db=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            dblogger.error(e)
            while not self.conn:
                self.conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
                time.sleep(1)

    def get_conn_instance(self):
        try:
            conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            conn = None
            dblogger.error(e)
            while not conn:
                conn = pymysql.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
                time.sleep(1)
        return conn

    def reset_auto_increment_id(self, tablename):
        sql = f'''
        alter table {tablename} drop id;
        alter table {tablename} add id int primary key not null auto_increment first;
        '''
        self.sql_execute(sql)
        # dblogger.warning(f'{sql}')


        
            

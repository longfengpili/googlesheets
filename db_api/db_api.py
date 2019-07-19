'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-07-19 16:54:28
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
        if id_min and id_max:
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

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            dblogger.error(e)

class DBMysql(DBFunction):
    def __init__(self, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        try:
            self.conn = pymysql.connect(
                db=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            self.conn = None
            dblogger.error(e)

    def reset_auto_increment_id(self, tablename):
        sql = f'''
        alter table {tablename} drop id;
        alter table {tablename} add id int primary key not null auto_increment first;
        '''
        self.sql_execute(sql)
        # dblogger.warning(f'{sql}')


        
            

'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-06-28 18:36:20
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from datetime import date, timedelta, datetime
import re
import sys

import pandas as pd
from pandas import DataFrame
import logging
import logging.handlers

#1.创建logger
dblogger = logging.getLogger(name='db')
dblogger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './log/db.log'
fh = logging.handlers.TimedRotatingFileHandler(
    logfile, when='D', interval=1, backupCount=100, encoding='utf-8')
fh.setLevel(logging.ERROR)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
dblogger.addHandler(fh)
dblogger.addHandler(ch)


class DBBase(object):
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
    
    def connect(self):
        pass

    def __close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __check_sql_type(self, sql):
        result = re.match('(\D.*?) ', sql)
        return result.group(1)

    def __join_values(self, values):
        '''拼凑values, "、' 不同方式处理'''
        values_ = ''.join([str(i) for value in values for i in value])
        search_1, search_2 = re.search("'", values_), re.search('"', values_)
        # print(search_1, search_2, values_)
        if not values_:
            pass
        elif search_1 and not search_2:
            values = ',\n'.join(['(' + ','.join([f'"{i}"' for i in value]) + ')' for value in values])
        elif search_2 and not search_1:
            values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        elif not (search_1 and search_2):
            values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        else:
            dblogger.error(values_)
            raise 'The values have some value use both "\'" and \'"\' !' 
        return values

    def sql_execute(self, sql, count=None):
        change_count = 0
        if not sql:
            return None, None
        # print(sql)
        if not self.conn:
            self.connect()
        sql_type = self.__check_sql_type(sql)
        result = f'{sql_type} completed !'

        cursor = self.conn.cursor()
        if sql_type == 'select':
            if count:
                cursor.execute(sql)
                change_count = cursor.rowcount
                result = cursor.fetchmany(count)
            else:
                cursor.execute(sql)
                change_count = cursor.rowcount
                result = cursor.fetchall()
        else:
            try:
                cursor.execute(sql)
                change_count = cursor.rowcount
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                dblogger.error(sql)
                dblogger.error(e)

        self.__close()
        return change_count,result

    def sql_for_create(self, tablename, columns):
        if not isinstance(columns, dict):
            raise 'colums must be a dict ! example:{"column_name":"column_type"}'
        sql = f'''create table if not exists {tablename}
                    ({','.join([k.lower() + ' '+ v for k, v in columns.items()])});'''

        sql = re.sub('\s{2,}', '\n', sql)
        return sql

    def sql_for_drop(self, tablename):
        sql = f'drop table if exists {tablename};'
        return sql

    def sql_for_insert(self, tablename, columns, values):
        sql = None
        columns = ','.join(columns)

        values = self.__join_values(values)
        
        if values:
            values = values.replace('"Null"', 'Null').replace("'Null'", 'Null')
            sql = f'''insert into {tablename}
                    ({columns})
                    values
                    {values};'''
        return sql

    def sql_for_select(self, tablename, columns, contions=None):
        columns = ','.join(columns)
        if contions:
            sql = f'''select {columns} from {tablename} where {contions};'''
        else:
            sql = f'''select {columns} from {tablename};'''
        return sql

    def sql_for_delete(self, tablename, columns, contions):
        columns = ','.join(columns)
        sql = f'''delete from {tablename} where {contions};'''
        return sql

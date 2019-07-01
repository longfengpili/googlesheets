'''
@Author: longfengpili
@Date: 2019-06-28 11:05:49
@LastEditTime: 2019-06-28 20:25:59
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re
from db_api.db_api import DBMysql
from params import *

# import logging
# import logging.handlers

# #1.创建logger
# resolve_logger = logging.getLogger(name='resolve_bi')
# resolve_logger.setLevel(logging.INFO)
# #2.创建handler写入日志
# logfile = './log/resolve_bi.log'
# fh = logging.handlers.TimedRotatingFileHandler(
#     logfile, when='D', interval=1, backupCount=100, encoding='utf-8')
# fh.setLevel(logging.WARNING)
# #3.创建handler输出控制台
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# #4.创建格式
# LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
# formatter = logging.Formatter(fmt=LOG_FORMAT)
# fh.setFormatter(formatter)
# ch.setFormatter(formatter)
# #5.将handler加入到logger
# resolve_logger.addHandler(fh)
# resolve_logger.addHandler(ch)

class ResolveMysqlData(object):
    def __init__(self, host, user, password, database, resolve_columns):
        self.resolve_tableid = None
        self.new_tableid = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.resolve_columns = resolve_columns

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, db=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def get_table_id(self, resolve_tablename, new_tablename, column='id', func='max'):
        '''获取两个表的最大id，用于后续对比，并逐步导出'''
        if not self.db:
            self._mysql_connect()
        if not self.resolve_tableid:
            self.resolve_tableid = self.db.get_table_info(resolve_tablename, column=column, func=func)
        if not self.new_tableid:
            self.new_tableid = self.db.get_table_info(new_tablename, column=column, func=func)

    def get_non_resolve_data(self, tablename, columns, n=1000):
        '''获取没有拆解的数据'''
        self._mysql_connect()
        if self.resolve_tableid < self.new_tableid:
            start_id = self.resolve_tableid
            end_id = self.resolve_tableid + n
            if end_id >= self.new_tableid:
                end_id = self.new_tableid
            sql = self.db.sql_for_select(tablename=tablename, columns=columns,
                                         contions=f'id > {start_id} and id <= {end_id}')                 
            count, non_resolve_data = self.db.sql_execute(sql)
            self.count += count
            self.resolve_tableid = end_id
        return non_resolve_data

    def resolve_row(self,row):
        # print(row)
        columns_value = []
        id, data_json = row
        data_json = json.loads(data_json)
        for column in self.resolve_columns:
            if column.endswith('ts'):
                try:#解决传文本的数据
                    locals()[column] = round(data_json.get(column, 0))
                except:
                    locals()[column] = 0
                if 0 < locals()[column] <= 19912435199: #2600-12-31
                    locals()[column] = datetime.utcfromtimestamp(locals()[column]).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    locals()[column] = datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S')
            else:
                locals()[column] = data_json.get(column, None)
                if not locals()[column]:
                    locals()[column] = 'Null'
                    
            columns_value.append(str(locals()[column]))
        row = columns_value
        # print(row)
        return row

    def resolve_multiple_rows(self,rows):
        resolved = []
        for row in rows:
            row = self.resolve_row(row)
            resolved.append(row)
        return resolved








        
    

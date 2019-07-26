'''
@Author: longfengpili
@Date: 2019-06-28 11:05:49
@LastEditTime: 2019-07-26 17:12:10
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re
from db_api.db_api import DBMysql, DBRedshift
from .parse_bi_func import ParseBiFunc

import logging
from logging import config

config.fileConfig('parselog.conf')
resolvebi_logger = logging.getLogger('resolvebi')
parsebi_logger = logging.getLogger('parsebi')

import threading
lock = threading.Lock() #生成全局锁
from .mythread import MyThread
import time


class ResolveData(ParseBiFunc):
    def __init__(self, host, user, password, database, orignal_columns, resolve_columns, db_type):
        self.table_id = None
        self.table2_id = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.orignal_columns = orignal_columns
        self.resolve_columns = resolve_columns
        self.db_type = db_type

    def _connect(self):
        if self.db_type == 'mysql':
            if not self.db:
                self.db = DBMysql(host=self.host, user=self.user,
                                password=self.password, database=self.database)
            if not self.conn:
                self.conn = self.db.connect()
        elif self.db_type == 'redshift':
            if not self.db:
                self.db = DBRedshift(host=self.host, user=self.user,
                                     password=self.password, database=self.database)
            if not self.conn:
                self.conn = self.db.connect()

    def resolve_row(self,row):
        # print(row)
        columns_value = []
        id, data_json = row
        data_json = json.loads(data_json)
        for column in self.resolve_columns:
            if column.endswith('ts') or column.endswith('_at'):
                try:#解决传文本的数据
                    locals()[column] = round(int(data_json.get(column, 0)))
                except:
                    locals()[column] = 0
                if 0 < locals()[column] <= 19912435199: #2600-12-31
                    locals()[column] = datetime.utcfromtimestamp(locals()[column]).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    locals()[column] = 'Null'
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

    def resolve_data_once(self, repair_tablename, resolve_tablename, n=1000):
        #获取未修复数据
        with lock:
            data, start_id, end_id = self.get_data(tablename1=repair_tablename, columns=self.orignal_columns, n=n)
        #修复数据
        resolved = self.resolve_multiple_rows(data)
        # print(resolved[0])
        sql = self.db.sql_for_insert(tablename=resolve_tablename, columns=self.resolve_columns, values=resolved)
        self.sql_execute_by_instance(self.db, sql)
        parsebi_logger.info(f'本次累计解析【({start_id},{end_id}]】{end_id - start_id}条数据！')

    def resolve_data_main(self, repair_tablename, resolve_tablename, id_min=None, id_max=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            repair_tablename:修正后数据表名
            resolve_tablename:拆解后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
        @return: 修改并解析数据，无返回值
        '''
        parsebi_logger.info(f'开始解析数据 ！on 【{self.host[:16]}】')
        n = 1000
        self._connect()
        self.db.create_table(resolve_tablename, columns=self.resolve_columns)

        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            id_min -= 1  # 左开右闭
            #删除resolve表数据
            self.db.delete_by_id(tablename=resolve_tablename, id_min=id_min, id_max=id_max)

        # resolve_table
        self.get_tables_id_single_db(tablename1=repair_tablename, tablename2=resolve_tablename)
        if not id_max:
            id_max = self.table_id
        if id_min:
            self.table2_id = id_min if id_min >= 0 else 0
            self.table_id = self.table_id if self.table_id <= id_max else id_max
        parsebi_logger.info(f'开始解析数据【({self.table2_id},{self.table_id}]】, 共【{self.table_id - self.table2_id}】条！')
        start_id = self.table2_id
        while self.table2_id < self.table_id:
            # self.resolve_data_once(orignal_tablename, repair_tablename, n=n)
            threads = []
            for i in range(10):
                if self.table2_id + n * i < self.table_id:
                    args = (repair_tablename, resolve_tablename)
                    t = MyThread(self.resolve_data_once, *args, n=n)
                    threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        parsebi_logger.info(f'本次累计解析【({start_id},{self.table_id}]】, 共【{self.table_id - start_id}】条！')


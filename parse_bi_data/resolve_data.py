'''
@Author: longfengpili
@Date: 2019-06-28 11:05:49
@LastEditTime: 2019-09-24 17:38:03
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-


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
    def __init__(self, host, user, password, database, original_columns, resolve_columns, no_resolve_columns, db_type):
        self.table_id = None
        self.table2_id = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.original_columns = original_columns
        self.resolve_columns = resolve_columns
        self.no_resolve_columns = no_resolve_columns
        self.db_type = db_type

    def _connect(self):
        if self.db_type == 'mysql':
            if not self.db:
                self.db = DBMysql(host=self.host, user=self.user,
                                password=self.password, database=self.database)
            if not self.conn:
                self.conn = self.db._connect()
        elif self.db_type == 'redshift':
            if not self.db:
                self.db = DBRedshift(host=self.host, user=self.user,
                                     password=self.password, database=self.database)
            if not self.conn:
                self.conn = self.db._connect()


    def get_field_value(self, log, field):
        value = log.get(field, 'Null')
        value = 'Null' if value == '' else value
        if field.endswith('ts') or field.endswith('_at'):
            value_ = int(str(value)[:10]) if len(str(value)) > 10 else value
            if isinstance(value_, int):
                value = datetime.utcfromtimestamp(value_)
        return value

    def resolve_row(self,row):
        # print(row)
        columns_value = []
        need_columns = set()
        id, data_json = row
        columns_value.append(id)
        data_json = json.loads(data_json)
        for column in self.resolve_columns:
            if column != 'id':
                value = self.get_field_value(data_json, column)
                columns_value.append(value)
                
        keys = set(data_json) - set(self.resolve_columns) - set(self.no_resolve_columns)
        if keys:
            key_ = {}
            for key in keys:
                need_columns.add(key)
                key_[key] = data_json.get(key)
            parsebi_logger.error(f'【{id}】【{key_}】 do not parse, if need please change your resolve columns !')
        return need_columns, columns_value

    def resolve_multiple_rows(self,rows):
        resolved = []
        need_columns_all = set()
        for row in rows:
            need_columns, row = self.resolve_row(row)
            resolved.append(row)
            need_columns_all.update(need_columns)
        if need_columns_all:
            parsebi_logger.error('\n' + '⭐'*40 + f'\n【{need_columns_all}】 do not parse, if need please change your resolve columns !\n' + '⭐'*40)
        return resolved

    def resolve_data_once(self, repair_tablename, resolve_tablename, n=1000):
        #获取未修复数据
        # with lock:
        data, start_id, end_id = self.get_data(db=self.db, tablename1=repair_tablename, columns=self.original_columns, n=n)
        #修复数据
        resolved = self.resolve_multiple_rows(data)
        # print(resolved[0])
        sql = self.db.sql_for_insert(tablename=resolve_tablename, columns=self.resolve_columns, values=resolved)
        count, data = self.sql_execute_by_instance(self.db, sql)
        if count != None and count > 0:
            self.count += count
            parsebi_logger.info(f'本次解析【({start_id},{end_id}]】{count}条数据！')
        else:
            parsebi_logger.error(f'本次解析【({start_id},{end_id}]】失败！')

    def resolve_data_main(self, repair_tablename, resolve_tablename, id_min=None, id_max=None, n=1000):
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
        self._connect()
        if id_min and id_min <= 1:
            self.db.drop_table(resolve_tablename)
        self.db.create_table(resolve_tablename, columns=self.resolve_columns)

        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            #删除resolve表数据
            self.db.delete_by_id(tablename=resolve_tablename, id_min=id_min, id_max=id_max)
            id_min -= 1  # 左开右闭
            

        # resolve_table
        self.get_tables_id_single_db(tablename1=repair_tablename, tablename2=resolve_tablename)
        if not id_max:
            id_max = self.table_id
        if id_min or id_min == 0:
            self.table2_id = id_min if id_min >= 0 else 0
            self.table_id = self.table_id if self.table_id <= id_max else id_max
        
        counts = self.table_id - self.table2_id
        parsebi_logger.info(f'开始解析数据【({self.table2_id},{self.table_id}]】, 共【{counts}】条！')
        start_id = self.table2_id
        while self.table2_id < self.table_id:
            # self.resolve_data_once(original_tablename, repair_tablename, n=n)
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
        
        if counts == self.count:
            parsebi_logger.info(f'本次累计解析【({start_id},{self.table_id}]】, 共【{self.count}】条！')
        else:
            parsebi_logger.error(f'本次累计解析【({start_id},{self.table_id}]】, 预计【{counts}】条，实际【{self.count}】条！')
            

'''
@Author: longfengpili
@Date: 2019-06-28 11:05:49
@LastEditTime: 2019-07-01 13:00:17
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re
from db_api.db_api import DBMysql

import logging
from logging import config

config.fileConfig('parselog.conf')
resolvebi_logger = logging.getLogger('resolvebi')
parsebi_logger = logging.getLogger('parsebi')

class ResolveMysqlData(object):
    def __init__(self, host, user, password, database, orignal_columns, resolve_columns):
        self.resolve_tableid = None
        self.repair_tableid = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.orignal_columns = orignal_columns
        self.resolve_columns = resolve_columns

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def get_table_id(self, resolve_tablename, repair_tablename, column='id', func='max'):
        '''获取两个表的最大id，用于后续对比，并逐步导出'''
        if not self.db:
            self._mysql_connect()
        if not self.resolve_tableid:
            self.resolve_tableid = self.db.get_table_info(resolve_tablename, column=column, func=func)
        if not self.repair_tableid:
            self.repair_tableid = self.db.get_table_info(repair_tablename, column=column, func=func)

    def get_non_resolve_data(self, tablename, columns, n=1000):
        '''获取没有拆解的数据'''
        self._mysql_connect()
        if self.resolve_tableid < self.repair_tableid:
            start_id = self.resolve_tableid
            end_id = self.resolve_tableid + n
            if end_id >= self.repair_tableid:
                end_id = self.repair_tableid
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

    def resolve_mysql_main(self, repair_tablename, resolve_tablename, id_min=None, id_max=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            repair_tablename:修正后数据表名
            resolve_tablename:拆解后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
        @return: 修改并解析数据，无返回值
        '''
        if id_min and id_max:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min:
            id_min -= 1  # 左开右闭

            #删除resolve表数据
            self._mysql_connect()
            self.db.delete_by_id(tablename=resolve_tablename, id_min=id_min, id_max=id_max)

        # resolve_table
        self.get_table_id(resolve_tablename, repair_tablename)
        if id_min:
            self.resolve_tableid = id_min
            self.repair_tableid = self.repair_tableid if self.repair_tableid <= id_max else id_max
            parsebi_logger.info(f'开始修复丢失数据【({self.resolve_tableid},{self.repair_tableid}]】 ！')
        while self.resolve_tableid < self.repair_tableid:
            #获取未修复数据
            non_resolve_data = self.get_non_resolve_data(tablename=repair_tablename, columns=self.orignal_columns, n=1000)
            #修复数据
            resolveed = self.resolve_multiple_rows(non_resolve_data)
            #插入新表
            sql = self.db.sql_for_insert(tablename=resolve_tablename, columns=self.resolve_columns, values=resolveed)
            self.db.sql_execute(sql)
            parsebi_logger.info(f'本次累计修复{self.count}条数据！最大id为{self.resolve_tableid} ！')







        
    

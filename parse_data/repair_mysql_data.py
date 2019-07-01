'''
@Author: longfengpili
@Date: 2019-06-27 14:41:34
@LastEditTime: 2019-07-01 18:39:46
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re
from db_api import DBMysql

import logging
from logging import config

config.fileConfig('parselog.conf')
repairbi_logger = logging.getLogger('repairbi')
parsebi_logger = logging.getLogger('parsebi')

class RepairMysqlData(object):
    def __init__(self, host, user, password, database, orignal_columns):
        self.repair_tableid = None
        self.orignal_tableid = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.orignal_columns = orignal_columns

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def get_table_id(self, repair_tablename, orignal_tablename,column='id',func='max'):
        '''获取两个表的最大id，用于后续对比，并逐步导出'''
        if not self.db:
            self._mysql_connect()
        if not self.repair_tableid:
            self.repair_tableid = self.db.get_table_info(repair_tablename, column=column, func=func)
        if not self.orignal_tableid:
            self.orignal_tableid = self.db.get_table_info(orignal_tablename, column=column, func=func)

    def get_non_repair_data(self, tablename, columns, n=1000):
        '''获取没有修复的数据'''
        self._mysql_connect()
        if self.repair_tableid < self.orignal_tableid:
            start_id = self.repair_tableid
            end_id = self.repair_tableid + n
            if end_id >= self.orignal_tableid:
                end_id = self.orignal_tableid
            sql = self.db.sql_for_select(tablename=tablename, columns=columns,
                                         contions=f'id > {start_id} and id <= {end_id}')
            count,non_repair_data = self.db.sql_execute(sql)
            self.count += count
            self.repair_tableid = end_id
        return non_repair_data

    def repair_row(self, row):
        '''修复单行数据'''
        e_n = 0
        errors = []
        id, myjson = row
        try:
            myjson_ = myjson
            myjson = json.loads(myjson)
        except Exception as e:
            l = '>' * ((30 - len(str(id)))//2)
            l_ = '<' * ((30 - len(str(id)))//2)
            msg_type = re.search('"msg_type":"(.*?)"', str(myjson))
            if msg_type:
                msg_type = msg_type.group(1)
            else:
                msg_type = 'ERROR'
                repairbi_logger.error(f'不存在msg_type!\n{row}')

            errors.append(f'\n{l}【{msg_type}】{l_}【{id}】')
            e_s = f'>>>>{e}'

            while e_s:
                errors.append(e_s)
                if 'UTF-8 BOM' in e_s:
                    myjson = myjson.encode('utf-8')[3:].decode('utf-8')
                    try:
                        myjson_ = myjson
                        myjson = json.loads(myjson)
                        e_s = None
                    except Exception as e:
                        e_s = f'>>>>{e}'
                        myjson = myjson
                elif "Expecting ',' delimiter" in e_s:
                    myjson = myjson.replace('":"{"', '":{"').replace('}","', '},"')
                    try:
                        myjson_ = myjson
                        myjson = json.loads(myjson)
                        e_s = None
                    except Exception as e:
                        e_s = f'>>>>{e}'
                        myjson = myjson
                e_n += 1
                if e_s and e_n == 5:  # 解析不了返回None
                    repairbi_logger.error(myjson)
                    myjson = None
                    break
            myjson = myjson_
            repairbi_logger.warning('\n'.join(errors))
            return id, myjson, errors
        if errors:
            repairbi_logger.warning('\n'.join(errors))
        myjson = myjson_
        return id, myjson, errors

    def repair_multiple_rows(self, rows):
        repaired = []
        for row in rows:
            id, myjson, _ = self.repair_row(row)
            r_l = [id, myjson]
            # repairbi_logger.info(r_l)
            repaired.append(r_l)
        return repaired

    def repair_mysql_main(self, orignal_tablename, repair_tablename, id_min=None, id_max=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            orignal_tablename:原始数据表名
            repair_tablename:修复后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
        @return: 修改并解析数据，无返回值
        '''

        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            id_min -= 1  # 左开右闭
            #删除repair表数据
            self._mysql_connect()
            self.db.delete_by_id(tablename=repair_tablename, id_min=id_min, id_max=id_max)
        
        # repair_table
        self.get_table_id(repair_tablename, orignal_tablename)
        if id_min:
            self.repair_tableid = id_min
            self.orignal_tableid = self.orignal_tableid if self.orignal_tableid <= id_max else id_max
            parsebi_logger.info(f'开始修复丢失数据【[{self.repair_tableid + 1},{self.orignal_tableid}]】 ！')
        while self.repair_tableid < self.orignal_tableid:
            #获取未修复数据
            non_repair_data = self.get_non_repair_data(tablename=orignal_tablename, columns=self.orignal_columns, n=1000)
            #修复数据
            repaired = self.repair_multiple_rows(non_repair_data)
            #插入新表
            sql = self.db.sql_for_insert(tablename=repair_tablename, columns=self.orignal_columns, values=repaired)
            self.db.sql_execute(sql)
            parsebi_logger.info(f'本次累计修复{self.count}条数据！最大id为{self.repair_tableid} ！')





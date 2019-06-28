'''
@Author: longfengpili
@Date: 2019-06-27 14:41:34
@LastEditTime: 2019-06-28 19:32:47
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re

from db_api import DBMysql
from params import *

import logging
import logging.handlers

#1.创建logger
errorbi_logger = logging.getLogger(name='error_bi')
errorbi_logger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './log/error_bi.log'
fh = logging.handlers.TimedRotatingFileHandler(
    logfile, when='D', interval=1, backupCount=100, encoding='utf-8')
fh.setLevel(logging.WARNING)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
errorbi_logger.addHandler(fh)
errorbi_logger.addHandler(ch)

class RepairMysqlData(object):
    def __init__(self, host, user, password, database):
        self.new_tableid = None
        self.old_tableid = None
        self.count = 0
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, db=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def get_table_id(self, new_tablename, old_tablename,column='id',func='max'):
        '''获取两个表的最大id，用于后续对比，并逐步导出'''
        if not self.db:
            self._mysql_connect()
        if not self.new_tableid:
            self.new_tableid = self.db.get_table_info(new_tablename, column=column, func=func)
        if not self.old_tableid:
            self.old_tableid = self.db.get_table_info(old_tablename, column=column, func=func)

    def get_non_repair_data(self, tablename, columns, n=1000):
        '''获取没有修复的数据'''
        self._mysql_connect()
        if self.new_tableid < self.old_tableid:
            start_id = self.new_tableid
            end_id = self.new_tableid + n
            if end_id >= self.old_tableid:
                end_id = self.old_tableid
            sql = self.db.sql_for_select(tablename=tablename, columns=columns,
                                         contions=f'id > {start_id} and id <= {end_id}')
            count,non_repair_data = self.db.sql_execute(sql)
            if count == 0:
                count = 1
            self.count += count
            self.new_tableid += count
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
            msg_type = re.search('"msg_type":"(.*?)"', str(myjson)).group(1)
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
                    errorbi_logger.error(myjson)
                    myjson = None
                    break
            myjson = myjson_
            errorbi_logger.warning('\n'.join(errors))
            return id, myjson, errors
        if errors:
            errorbi_logger.warning('\n'.join(errors))
        myjson = myjson_
        return id, myjson, errors

    def repair_multiple_rows(self, rows):
        repaired = []
        for row in rows:
            id, myjson, _ = self.repair_row(row)
            r_l = [id, myjson]
            # errorbi_logger.info(r_l)
            repaired.append(r_l)
        return repaired

        
        

'''
@Author: longfengpili
@Date: 2019-06-27 14:41:34
@LastEditTime: 2019-06-27 19:53:46
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

import logging

#1.创建logger
errorbi_logger = logging.getLogger(name='error_bi')
errorbi_logger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './error_bi.log'
fh = logging.FileHandler(logfile, mode='a')
fh.setLevel(logging.ERROR)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
errorbi_logger.addHandler(fh)
errorbi_logger.addHandler(ch)

class RepairMysqlData(object):
    def __init__(self):
        self.repair_table_id = None
        self.old_table_id = None
        self.count = 0
        self.db = None
        self.conn = None

    def _mysql_connect(self, host=M_HOST, user=M_USER,password=M_PASSWORD, db=M_DATABASE):
        if not self.db:
            self.db = DBMysql(host=host, user=user, password=password, db=db)
        if not self.conn:
            self.conn = self.db.connect()

    def get_table_id(self):
        # 获取两个表的最大id，用于后续对比，并逐步导出
        if not self.repair_table_id:
            self._mysql_connect()
            sql = self.db.sql_for_select(tablename=M_N_TABLENAME,columns=['id'])
            result = self.db.sql_execute(sql)
            if not result:
                result = 0
            else:
                result = max(result)[0]
            self.repair_table_id = result
        if not self.old_table_id:
            self._mysql_connect()
            sql = self.db.sql_for_select(tablename=M_O_TABLENAME, columns=['id'])
            result = self.db.sql_execute(sql)
            if not result:
                result = 0
            else:
                result = max(result)[0]
            self.old_table_id = result

    def get_non_repair_data(self, n=1000):
        # 获取没有修复的数据
        self._mysql_connect()
        if self.repair_table_id < self.old_table_id:
            sql = self.db.sql_for_select(tablename=M_O_TABLENAME, columns=M_COLUMNS,
                                         contions=f'where id > {self.repair_table_id} and id <= {self.repair_table_id + n}')
            non_repair_data = self.db.sql_execute(sql)
            self.repair_table_id += n
        return non_repair_data

    def repair_row(self, row):
        # 修复单行数据
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
                # errorbi_logger.info(e_s)
                # errorbi_logger.info(myjson)
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
            errorbi_logger.error('\n'.join(errors))
            return id, myjson, errors
            
        if errors:
            errorbi_logger.error('\n'.join(errors))
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

        
        

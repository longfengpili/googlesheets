'''
@Author: longfengpili
@Date: 2019-06-27 14:41:34
@LastEditTime: 2019-07-12 14:46:31
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import json
import re
from db_api import DBMysql, DBRedshift
from .repair_data import RepairJsonData

import logging
from logging import config

config.fileConfig('parselog.conf')
repairbi_logger = logging.getLogger('repairbi')
parsebi_logger = logging.getLogger('parsebi')


class RepairMysqlDataToRedshift(object):
    def __init__(self, mysql_host, mysql_user, mysql_password, mysql_database, redshift_host, redshift_user, redshift_password, redshift_database, orignal_columns):
        self.repair_tableid = None
        self.orignal_tableid = None
        self.count = 0
        self.mysql_db = None
        self.mysql_conn = None
        self.mysql_host = mysql_host
        self.mysql_port = 3306
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.redshift_db = None
        self.redshift_conn = None
        self.redshift_host = redshift_host
        self.redshift_port = '5439'
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_database = redshift_database
        self.orignal_columns = orignal_columns

    def _mysql_connect(self):
        if not self.mysql_db:
            self.mysql_db = DBMysql(host=self.mysql_host, user=self.mysql_user,
                              password=self.mysql_password, database=self.mysql_database)
        if not self.mysql_conn:
            self.mysql_conn = self.mysql_db.connect()

    def _redshift_connect(self):
        if not self.redshift_db:
            self.redshift_db = DBRedshift(host=self.redshift_host, user=self.redshift_user,
                                   password=self.redshift_password, database=self.redshift_database)
        if not self.redshift_conn:
            self.redshift_conn = self.redshift_db.connect()

    def get_table_id(self, repair_tablename, orignal_tablename,column='id',func='max'):
        '''获取两个表的最大id，用于后续对比，并逐步导出'''
        self._redshift_connect()
        if not self.repair_tableid:
            sql = self.redshift_db.sql_for_column_agg(repair_tablename, column=column, func=func)
            _, result = self.redshift_db.sql_execute(sql)
            # print(result[0][0] if result[0][0] else 0 )
            self.repair_tableid = result[0][0] if result[0][0] else 0 
        self._mysql_connect()
        if not self.orignal_tableid:
            sql = self.mysql_db.sql_for_column_agg(orignal_tablename, column=column, func=func)
            _, result = self.mysql_db.sql_execute(sql)
            self.orignal_tableid = result[0][0] if result[0][0] else 0 

    def get_non_repair_data(self, tablename, columns, n=1000):
        '''获取没有修复的数据'''
        self._mysql_connect()
        if self.repair_tableid < self.orignal_tableid:
            start_id = self.repair_tableid
            end_id = self.repair_tableid + n
            if end_id >= self.orignal_tableid:
                end_id = self.orignal_tableid
            sql = self.mysql_db.sql_for_select(tablename=tablename, columns=columns,
                                         contions=f'id > {start_id} and id <= {end_id}')
            count,non_repair_data = self.mysql_db.sql_execute(sql)
            self.count += count
            self.repair_tableid = end_id
        return non_repair_data

    def repair_row(self, row):
        '''修复单行数据'''
        id, myjson = row
        rjd = RepairJsonData(myjson)
        myjson = rjd.repair_main()
        if rjd.error_num >= rjd.error_max:
            l = '>' * ((30 - len(str(id)))//2)
            l_ = '<' * ((30 - len(str(id)))//2)
            msg_type = re.search('"msg_type":"(.*?)"', str(rjd.myjson_origin))
            if msg_type:
                msg_type = msg_type.group(1)
            else:
                msg_type = 'ERROR'
                repairbi_logger.error(f'不存在msg_type!\n{row}')
                rjd.errors.insert(0, f'\n{l}【{msg_type}】{l_}【{id}】')
            error = '\n'.join(rjd.errors)
            repairbi_logger.error(f"{error}")
                    
        return id, myjson, rjd.errors

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
        parsebi_logger.info(f'开始修复数据 ！')
        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            id_min -= 1  # 左开右闭
            #删除repair表数据
            self._redshift_connect()
            self.redshift_db.delete_by_id(tablename=repair_tablename, id_min=id_min, id_max=id_max)
        
        # repair_table
        self.get_table_id(repair_tablename, orignal_tablename)
        if not id_max:
            id_max = self.orignal_tableid
        if id_min:
            self.repair_tableid = id_min
            self.orignal_tableid = self.orignal_tableid if self.orignal_tableid <= id_max else id_max
        parsebi_logger.info(f'开始修复数据【[{self.repair_tableid + 1},{self.orignal_tableid}]】, 共【{self.orignal_tableid - self.repair_tableid}】条！')
        while self.repair_tableid < self.orignal_tableid:
            #获取未修复数据
            non_repair_data = self.get_non_repair_data(tablename=orignal_tablename, columns=self.orignal_columns, n=1000)
            #修复数据
            repaired = self.repair_multiple_rows(non_repair_data)
            #插入新表
            sql = self.redshift_db.sql_for_insert(tablename=repair_tablename, columns=self.orignal_columns, values=repaired)
            self.redshift_db.sql_execute(sql)
            parsebi_logger.info(f'本次累计修复{self.count}条数据！最大id为{self.repair_tableid} ！')





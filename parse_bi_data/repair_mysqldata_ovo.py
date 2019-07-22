'''
@Author: longfengpili
@Date: 2019-06-27 14:41:34
@LastEditTime: 2019-07-22 18:42:15
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from datetime import datetime
import os
import json
import re
from db_api import DBMysql, DBRedshift
from .repair_json import RepairJsonData
from .parse_bi_func import ParseBiFunc

import logging
from logging import config

config.fileConfig('parselog.conf')
repairbi_logger = logging.getLogger('repairbi')
parsebi_logger = logging.getLogger('parsebi')


class RepairMysqlDataOVO(ParseBiFunc):
    def __init__(self, db_host, db_user, db_password, db_database, orignal_columns, db2_host=None, db2_user=None, db2_password=None, db2_database=None):
        self.db = None
        self.conn = None
        self.table_id = None
        self.db2 = None
        self.conn2 = None
        self.table2_id = None
        self.count = 0
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_database = db_database
        self.db2_host = db2_host
        self.db2_user = db2_user
        self.db2_password = db2_password
        self.db2_database = db2_database
        self.orignal_columns = orignal_columns

    def _connect(self):
        if not self.db:
            self.db = DBMysql(host=self.db_host, user=self.db_user,
                              password=self.db_password, database=self.db_database)
        if not self.conn:
            self.conn = self.db.connect()

        if self.db2_host:  
            if not self.db2:
                self.db2 = DBRedshift(host=self.db2_host, user=self.db2_user,
                                    password=self.db2_password, database=self.db2_database)
            if not self.conn2:
                self.conn2 = self.db2.connect()
        else:
            self.db2 = self.db
            self.conn2 = self.conn

    def create_table_for_auto_increment_id(self, tablename, suffix='idxu'):
        '''
        only support mysql !
        '''
        self._connect()
        sql = f"select column_name, column_type from information_schema.columns where table_name = '{tablename}';"
        _, result = self.db.sql_execute(sql)
        columns_name = dict(result)
        if not columns_name:
            original_tablename = tablename.split(f'_{suffix}')[0]
            sql = f"select column_name, column_type from information_schema.columns where table_name = '{original_tablename}';"
            _, result = self.db.sql_execute(sql)
            columns_name = dict(result)
            if 'id' not in columns_name:
                keys = ['id'] + list(columns_name.keys())
                values = ['int auto_increment'] + list(columns_name.values())
                columns_name = dict(zip(keys, values))
                sql = self.db.sql_for_create(
                    tablename=tablename, columns=columns_name)
                self.db.sql_execute(sql)
        return columns_name
    
    def copy_data_to_idtable(self, tablename, id_min=None, suffix='idxu'):
        '''
        copy数据
        '''
        count = 0
        parsebi_logger.info(f'【{tablename}】数据增加自增ID开始！ in 【{self.db_host[:16]}】')
        self._connect()
        if id_min != None and id_min >= 0:
            self.db.delete_by_id(tablename, id_min=id_min)
        
        if suffix in tablename:  # 为了避免truncate错误的表
            columns_name = self.create_table_for_auto_increment_id(tablename, suffix=suffix)
            columns_name.pop('id')
            original_tablename = tablename.split(f'_{suffix}')[0]
            
            original_tablename_count = self.db.get_table_count(original_tablename)
            tablename_count = self.db.get_table_count(tablename)
            
            if tablename_count < original_tablename_count:
                sql_copy = f'''insert into {tablename}
                ({','.join(columns_name)})
                select ({','.join(columns_name)})
                from {original_tablename}
                limit {tablename_count} , {original_tablename_count - tablename_count}
                '''
                count, _ = self.db.sql_execute(sql_copy)
                self.db.reset_auto_increment_id(tablename) #更新自增id
                parsebi_logger.info(
                    f'【{tablename}】数据增加自增ID结束,【({tablename_count},{original_tablename_count}]】导入{count}条,！')
                return count
                
        parsebi_logger.info(f'【{tablename}】数据增加自增ID结束,未进行任何操作！')
        return count

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

    def repair_data_main(self, orignal_tablename, repair_tablename, id_min=None, id_max=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            orignal_tablename:原始数据表名
            repair_tablename:修复后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
        @return: 修改并解析数据，无返回值
        '''
        parsebi_logger.info(f'开始修复数据 ！ 【{self.db_host[:16]}】 to 【{(self.db2_host if self.db2_host else self.db_host)[:16]}】')
        self._connect()
        self.db2.create_table(repair_tablename, columns=self.orignal_columns)

        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            id_min -= 1  # 左开右闭
            #删除repair表数据
            self.db2.delete_by_id(tablename=repair_tablename, id_min=id_min, id_max=id_max)
        
        # repair_table
        self.get_tables_id_double_db(tablename1=orignal_tablename, tablename2=repair_tablename)
        if not id_max:
            id_max = self.table_id
        if id_min:
            self.table2_id = id_min if id_min >= 0 else 0
            self.table_id = self.table_id if self.table_id <= id_max else id_max
        parsebi_logger.info(f'开始修复数据【({self.table2_id},{self.table_id}]】, 共【{self.table_id - self.table2_id}】条！')
        while self.table2_id < self.table_id:
            #获取未修复数据
            data = self.get_data(tablename1=orignal_tablename, columns=self.orignal_columns, n=1000)
            #修复数据
            repaired = self.repair_multiple_rows(data)
            #插入新表
            sql = self.db2.sql_for_insert(tablename=repair_tablename, columns=self.orignal_columns, values=repaired)
            self.db2.sql_execute(sql)
            parsebi_logger.info(f'本次累计修复{self.count}条数据！最大id为{self.table2_id} ！')

    def repair_adjust_data_main(self, orignal_tablename, repair_tablename, id_min=None, suffix=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            orignal_tablename:原始数据表名
            repair_tablename:修复后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
            suffix:表名后缀，原表为去掉后缀的表名，带后缀的表名是为了增加自增id
        @return: 修改并解析数据，无返回值
        '''
        
        if suffix:
            count = self.copy_data_to_idtable(tablename=orignal_tablename, id_min=id_min, suffix=suffix)
            if count > 0:
                self.repair_data_main(orignal_tablename, repair_tablename, id_min=id_min)
            else:
                parsebi_logger.info(f'【{orignal_tablename}】adjust new data {count} num, do nothing !')





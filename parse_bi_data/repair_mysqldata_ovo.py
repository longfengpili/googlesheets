'''
@Author: longfengpili
@Date: 2019-08-01 12:22:23
@LastEditTime: 2019-08-01 12:22:23
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python
# -*- coding:utf-8 -*-



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

import threading
lock = threading.Lock() #生成全局锁
from .mythread import MyThread
import time



class RepairMysqlDataOVO(ParseBiFunc):
    def __init__(self, db_host, db_user, db_password, db_database, original_columns, db2_host=None, db2_user=None, db2_password=None, db2_database=None):
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
        self.original_columns = original_columns

    def _connect(self):
        if not self.db:
            self.db = DBMysql(host=self.db_host, user=self.db_user,
                              password=self.db_password, database=self.db_database)
        if not self.conn:
            self.conn = self.db._connect()

        if self.db2_host:  
            if not self.db2:
                self.db2 = DBRedshift(host=self.db2_host, user=self.db2_user,
                                    password=self.db2_password, database=self.db2_database)
            if not self.conn2:
                self.conn2 = self.db2._connect()
        else:
            self.db2 = self.db
            self.conn2 = self.conn

    def create_table_add_id(self, tablename, suffix='idxu'):
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
                values = ['int'] + list(columns_name.values())
                columns_name = dict(zip(keys, values))
                sql = self.db.sql_for_create(
                    tablename=tablename, columns=columns_name)
                # print(sql)
                self.db.sql_execute(sql)
        return columns_name
    
    def copy_data_to_idtable(self, tablename, id_min=None, suffix='idxu'):
        '''
        copy数据
        '''
        count = 0
        parsebi_logger.info(f'【{tablename}】数据增加自增ID开始！ in 【{self.db_host[:16]}】')
        self._connect()
        columns_name = self.create_table_add_id(tablename, suffix=suffix)
        
        if id_min != None and id_min >= 0:
            self.db.delete_by_id(tablename, id_min=id_min)
        
        if suffix in tablename:  # 为了避免truncate错误的表
            columns_name.pop('id')
            original_tablename = tablename.split(f'_{suffix}')[0]
            
            original_tablename_count = self.db.get_table_count(original_tablename)
            tablename_count = self.db.get_table_count(tablename)
            
            if tablename_count < original_tablename_count:
                parsebi_logger.info(
                    f'【{tablename}】数据需要增加,【({tablename_count},{original_tablename_count}]】, 预计导入{original_tablename_count - tablename_count}条,！')
                sql_copy = f'''
                set @num = {tablename_count};
                insert into {tablename}
                (id, {','.join(columns_name)})
                select (@num:=@num+1), {','.join(columns_name)}
                from {original_tablename}
                limit {tablename_count} , {original_tablename_count - tablename_count};
                '''
                count, _ = self.db.sql_execute(sql_copy)
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
        if rjd.error_num >= rjd.error_max:  # 如果超过错误报警
            myjson = json.loads(myjson)
            myjson['id_index'] = id
            myjson = json.dumps(myjson)

            error = '\n'.join(rjd.errors)
            error = re.subn('】', f'】【{id}】', error, 1)[0]
            repairbi_logger.error(f"{error}")
        return id, myjson, rjd.errors

    def repair_multiple_rows(self, rows):
        st = time.time()
        repaired = []
        for row in rows:
            id, myjson, _ = self.repair_row(row)
            r_l = [id, myjson]
            # repairbi_logger.info(r_l)
            repaired.append(r_l)
        et = time.time()
        # parsebi_logger.info(f'cost {round(et - st, 4)} seconds')
        return repaired
    
    def repair_data_once(self, original_tablename, repair_tablename, n=1000):
        #获取未修复数据
        # with lock:
        data, start_id, end_id = self.get_data(db=self.db, tablename1=original_tablename, columns=self.original_columns, n=n)
        #修复数据
        repaired = self.repair_multiple_rows(data)
        # print(repaired[0])
        sql = self.db2.sql_for_insert(tablename=repair_tablename, columns=self.original_columns, values=repaired)
        count, data = self.sql_execute_by_instance(self.db2, sql)
        if count != None and count > 0:
            self.count += count
            parsebi_logger.info(f'本次修复【({start_id},{end_id}]】{count}条数据！')
        else:
            parsebi_logger.error(f'本次修复【({start_id},{end_id}]】失败！')
        
    def repair_data_main(self, original_tablename, repair_tablename, id_min=None, id_max=None, n=1000):
        '''
        @description: 处理格式并拆解
        @param {type} 
            original_tablename:原始数据表名
            repair_tablename:修复后的数据表名
            id_min:需要重新跑的id开始值
            id_max:需要重新跑的id结束值
        @return: 修改并解析数据，无返回值
        '''
        parsebi_logger.info(f'开始修复数据 ！ 【{self.db_host[:16]}】 to 【{(self.db2_host if self.db2_host else self.db_host)[:16]}】')
        self._connect()
        self.db2.create_table(repair_tablename, columns=self.original_columns)

        if id_min != None and id_max != None:
            if id_min >= id_max:
                raise 'id_min should < id_max'
        if id_min != None:
            #删除repair表数据
            self.db2.delete_by_id(tablename=repair_tablename, id_min=id_min, id_max=id_max)
            id_min -= 1  # 左开右闭
            
        
        # repair_table
        self.get_tables_id_double_db(tablename1=original_tablename, tablename2=repair_tablename)
        if not id_max:
            id_max = self.table_id
        if id_min or id_min == 0:
            self.table2_id = id_min if id_min >= 0 else 0
            self.table_id = self.table_id if self.table_id <= id_max else id_max

        counts = self.table_id - self.table2_id
        parsebi_logger.info(f'开始修复数据【({self.table2_id},{self.table_id}]】, 共【{counts}】条！')
        start_id = self.table2_id
        while self.table2_id < self.table_id:
            # self.repair_data_once(original_tablename, repair_tablename, n=n)
            threads = []
            for i in range(10):
                if self.table2_id + n * i < self.table_id:
                    args = (original_tablename, repair_tablename)
                    t = MyThread(self.repair_data_once, *args, n=n)
                    threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        if counts == self.count:
            parsebi_logger.info(f'本次累计修复【({start_id},{self.table_id}]】, 共【{self.count}】条！')
        else:
            parsebi_logger.error(f'本次累计修复【({start_id},{self.table_id}]】, 预计【{counts}】条，实际【{self.count}】条！')

    def repair_adjust_data_main(self, original_tablename, repair_tablename, id_min=None, suffix=None):
        '''
        @description: 处理格式并拆解
        @param {type} 
            original_tablename:原始数据表名
            repair_tablename:修复后的数据表名
            id_min:需要重新跑的id开始值
            suffix:表名后缀，原表为去掉后缀的表名，带后缀的表名是为了增加自增id
        @return: 修改并解析数据，无返回值
        '''
        
        if suffix:
            count = self.copy_data_to_idtable(tablename=original_tablename, id_min=id_min, suffix=suffix)
            if count > 0:
                self.repair_data_main(original_tablename, repair_tablename, id_min=id_min)
            else:
                parsebi_logger.info(f'【{original_tablename}】adjust new data {count} num, do nothing !')





'''
@Author: longfengpili
@Date: 2019-07-12 18:04:02
@LastEditTime: 2019-07-30 12:37:54
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from db_api import DBFunction
import logging
from logging import config

config.fileConfig('parselog.conf')
repairbi_logger = logging.getLogger('repairbi')
import threading
lock = threading.Lock() #生成全局锁

class ParseBiFunc(DBFunction):
    def __init__(self):
        self.db = None
        self.conn = None
        self.table_id = None
        self.db2 = None
        self.conn2 = None
        self.table2_id = None
        self.count = 0

    def _connect(self):
        pass

    def get_tables_id_double_db(self, tablename1, tablename2):
        self._connect()
        if not self.table_id:
            self.table_id = self.db.get_table_id(tablename1)
        if not self.table2_id:
            self.table2_id = self.db2.get_table_id(tablename2)

    def get_tables_id_single_db(self, tablename1, tablename2):
        self._connect()
        if not self.table_id:
            self.table_id = self.db.get_table_id(tablename1)
        if not self.table2_id:
            self.table2_id = self.db.get_table_id(tablename2)
            
    def get_data(self, db, tablename1, columns, n=1000):
        self._connect()
        with lock: 
            if self.table2_id < self.table_id:
                start_id = self.table2_id
                end_id = self.table2_id + n
                if end_id >= self.table_id:
                    end_id = self.table_id
                self.table2_id = end_id
        if start_id < end_id:
            sql = db.sql_for_select(tablename=tablename1, columns=columns, contions=f'id > {start_id} and id <= {end_id}')
            conn = db.get_conn_instance()
            count, data = db.sql_execute(sql, conn=conn)
            self.count += count
            
        return data, start_id, end_id

    def sql_execute_by_instance(self, db, sql):
        #插入新表
        conn = db.get_conn_instance()
        name = threading.current_thread().name
        # print(name,conn)
        count, data = db.sql_execute(sql, conn=conn)
        return count, data
        



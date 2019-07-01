'''
@Author: longfengpili
@Date: 2019-07-01 14:17:52
@LastEditTime: 2019-07-01 17:26:42
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api import DBMysql
from .parse_sql import ParseSql
import os

import logging
from logging import config
config.fileConfig('parselog.conf')
dailylogger = logging.getLogger('daily')

class DailyMain(DBMysql):
    def __init__(self, host=None, user=None, password=None, database=None, sql_path='./sql/'):
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.sql_path = sql_path

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def daily_execute(self, execute_order, **kw):
        ps = ParseSql(sql_path=self.sql_path)
        files_sqls = ps.get_files_sqls(**kw)
        self._mysql_connect()
        for sql_file in execute_order:
            sql_file = self.sql_path + sql_file + ".sql"
            sqls = files_sqls.get(sql_file)
            for sql in sqls:
                dailylogger.info(f'【{sql_file}】【{sql[0]}】begin execute！')
                dailylogger.debug(sql[1])
                count, result = self.db.sql_execute(sql[1])
                dailylogger.info(f'【{sql_file}】【{sql[0]}】executed！effect 【{count}】 rows！')

    
    

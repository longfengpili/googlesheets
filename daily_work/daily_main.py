'''
@Author: longfengpili
@Date: 2019-07-01 14:17:52
@LastEditTime: 2019-07-09 12:58:42
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api import DBMysql, DBRedshift
from .parse_sql import ParseSql
import os

import logging
from logging import config
config.fileConfig('parselog.conf')
dailylogger = logging.getLogger('daily')

class DailyMain(object):
    '''
    sqlpath:默认sql地址
        '''
    def __init__(self, sqlpath):
        self.sqlpath = sqlpath

    def _connect(self):
        pass

    def daily_execute_all(self, execute_order, **kw):
        '''
        @description: 执行所有的sql文件，按照顺序
        @param {type} 
            execute_order:执行顺序
            kw:sql中需要设定的参数
        @return: 无
        '''
        ps = ParseSql(sqlpath=self.sqlpath)
        files_sqls = ps.get_files_sqls(**kw)
        # print(files_sqls)
        self._connect()
        for sql_file in execute_order:
            sql_file = self.sqlpath + sql_file + ".sql"
            sqls = files_sqls.get(sql_file)
            for sql in sqls:
                dailylogger.info(f'【{sql_file}】【{sql[0]}】begin execute！')
                # dailylogger.debug(sql[1])
                count, result = self.db.sql_execute(sql[1])
                dailylogger.info(f'【{sql_file}】【{sql[0]}】executed！effect 【{count}】 rows！')

    def daily_execute_single(self, schema, **kw):
        '''
        @description: 执行所有的sql文件，按照顺序
        @param {type} 
            kw:sql中需要设定的参数
        @return: 无
        '''
        ps = ParseSql(sqlpath=self.sqlpath)
        self._connect()
        sql_file = self.sqlpath + schema + ".sql"
        sqls = ps.get_file_sqls(sql_file, **kw)
        for sql in sqls:
            dailylogger.info(f'【{sql_file}】【{sql[0]}】begin execute！')
            count, result = self.db.sql_execute(sql[1])
            dailylogger.info(f'【{sql_file}】【{sql[0]}】executed！effect 【{count}】 rows！')

class DailyMainMysql(DailyMain):
    def __init__(self, host, user, password, database, sqlpath):
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.sqlpath = sqlpath

    def _connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

class DailyMainRedshift(DailyMain):
    def __init__(self, host, user, password, database, sqlpath):
        self.db = None
        self.conn = None
        self.host = host
        self.port = '5439'
        self.user = user
        self.password = password
        self.database = database
        self.sqlpath = sqlpath

    def _connect(self):
        if not self.db:
            self.db = DBRedshift(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

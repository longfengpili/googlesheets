'''
@Author: longfengpili
@Date: 2019-07-01 14:17:52
@LastEditTime: 2019-07-02 16:29:39
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
    '''
    host:数据库地址
    user:数据库用户名
    password：数据库密码
    database:数据库库名
    sqlpath:默认sql地址
        '''
    def __init__(self, host=None, user=None, password=None, database=None, sqlpath='./sql/'):
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.sqlpath = sqlpath

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

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
        self._mysql_connect()
        for sql_file in execute_order:
            sql_file = self.sqlpath + sql_file + ".sql"
            sqls = files_sqls.get(sql_file)
            for sql in sqls:
                dailylogger.info(f'【{sql_file}】【{sql[0]}】begin execute！')
                dailylogger.debug(sql[1])
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
        files_sqls = ps.get_files_sqls(**kw)
        self._mysql_connect()
        sql_file = self.sqlpath + schema + ".sql"
        sqls = files_sqls.get(sql_file)
        for sql in sqls:
            dailylogger.info(f'【{sql_file}】【{sql[0]}】begin execute！')
            dailylogger.debug(sql[1])
            count, result = self.db.sql_execute(sql[1])
            dailylogger.info(
                f'【{sql_file}】【{sql[0]}】executed！effect 【{count}】 rows！')

    
    

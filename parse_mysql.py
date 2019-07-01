'''
@Author: longfengpili
@Date: 2019-06-27 12:26:40
@LastEditTime: 2019-07-01 12:55:34
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api import DBMysql
from parse_data import RepairMysqlData
from parse_data import ResolveMysqlData
import sys
import time
from psetting import *

import logging
from logging import config

config.fileConfig('parselog.conf')
parsebi_logger = logging.getLogger('parsebi')



# 创建repair_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
sql = db.sql_for_create(tablename= M_REPAIR_TABLENAME,columns=M_ORIGINAL_COLUMNS)
db.sql_execute(sql)

# repair_table
rmd = RepairMysqlData(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE,orignal_columns=M_ORIGINAL_COLUMNS)
rmd.get_table_id(M_REPAIR_TABLENAME, M_ORIGINAL_TABLENAME)
while rmd.repair_tableid < rmd.orignal_tableid:
    #获取未修复数据
    non_repair_data = rmd.get_non_repair_data(tablename=M_ORIGINAL_TABLENAME, columns=M_ORIGINAL_COLUMNS, n=1000)
    #修复数据
    repaired = rmd.repair_multiple_rows(non_repair_data)
    # print(repaired)
    #插入新表
    sql = db.sql_for_insert(tablename=M_REPAIR_TABLENAME,columns=M_ORIGINAL_COLUMNS, values=repaired)
    db.sql_execute(sql)
    parsebi_logger.info(f'本次累计修复{rmd.count}条数据！最大id为{rmd.repair_tableid} ！')



# 创建resolve_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
sql = db.sql_for_create(tablename= M_RESOLVE_TABLENAME,columns=M_RESOLVE_COLUMNS)
db.sql_execute(sql)

# resolve_table
rsmd = ResolveMysqlData(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE, original_columns=M_ORIGINAL_COLUMNS, resolve_columns=M_RESOLVE_COLUMNS)
rsmd.get_table_id(M_RESOLVE_TABLENAME, M_REPAIR_TABLENAME)
while rsmd.resolve_tableid < rsmd.repair_tableid:
    #获取未拆分数据
    non_resolve_data = rsmd.get_non_resolve_data(tablename=M_REPAIR_TABLENAME, columns=M_ORIGINAL_COLUMNS, n=1000)
    #拆分数据
    resolved = rsmd.resolve_multiple_rows(non_resolve_data)
    # print(resolved)
    #插入新表
    sql = db.sql_for_insert(tablename=M_RESOLVE_TABLENAME,columns=M_RESOLVE_COLUMNS, values=resolved)
    db.sql_execute(sql)
    parsebi_logger.info(f'本次累计解析{rsmd.count}条数据！最大id为{rsmd.resolve_tableid} ！')

'''
@Author: longfengpili
@Date: 2019-06-27 12:26:40
@LastEditTime: 2019-06-28 20:23:46
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api import DBMysql
from parse_data import RepairMysqlData
from parse_data import ResolveMysqlData
import sys
import pandas as pd
from params import *
import time

import logging
import logging.handlers

#1.创建logger
parsebi_logger = logging.getLogger(name='parsebi_bi')
parsebi_logger.setLevel(logging.INFO)
#2.创建handler写入日志
logfile = './log/parse_bi.log'
fh = logging.handlers.TimedRotatingFileHandler(
    logfile, when='D', interval=1, backupCount=100, encoding='utf-8')
fh.setLevel(logging.INFO)
#3.创建handler输出控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#4.创建格式
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(lineno)d行 - %(message)s"
formatter = logging.Formatter(fmt=LOG_FORMAT)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#5.将handler加入到logger
parsebi_logger.addHandler(fh)
parsebi_logger.addHandler(ch)


# 创建repair_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, db=M_DATABASE)
sql = db.sql_for_create(tablename= M_N_TABLENAME,columns=M_COLUMNS)
db.sql_execute(sql)

# repair_table
rmd = RepairMysqlData(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
rmd.get_table_id(M_N_TABLENAME, M_O_TABLENAME)
while rmd.new_tableid < rmd.old_tableid:
    #获取未修复数据
    non_repair_data = rmd.get_non_repair_data(tablename=M_O_TABLENAME, columns=M_COLUMNS, n=1000)
    #修复数据
    repaired = rmd.repair_multiple_rows(non_repair_data)
    # print(repaired)
    #插入新表
    sql = db.sql_for_insert(tablename=M_N_TABLENAME,columns=M_COLUMNS, values=repaired)
    db.sql_execute(sql)
    parsebi_logger.info(f'本次累计修复{rmd.count}条数据！最大id为{rmd.new_tableid} ！')



# 创建resolve_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, db=M_DATABASE)
sql = db.sql_for_create(tablename= M_R_TABLENAME,columns=M_R_COLUMNS)
db.sql_execute(sql)

# resolve_table
rsmd = ResolveMysqlData(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE, resolve_columns=M_R_COLUMNS)
rsmd.get_table_id(M_R_TABLENAME, M_N_TABLENAME)
while rsmd.resolve_tableid < rsmd.new_tableid:
    #获取未拆分数据
    non_resolve_data = rsmd.get_non_resolve_data(tablename=M_N_TABLENAME, columns=M_COLUMNS, n=1000)
    #拆分数据
    resolved = rsmd.resolve_multiple_rows(non_resolve_data)
    # print(resolved)
    #插入新表
    sql = db.sql_for_insert(tablename=M_R_TABLENAME,columns=M_R_COLUMNS, values=resolved)
    db.sql_execute(sql)
    parsebi_logger.info(f'本次累计解析{rsmd.count}条数据！最大id为{rsmd.resolve_tableid} ！')

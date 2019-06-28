'''
@Author: longfengpili
@Date: 2019-06-27 12:26:40
@LastEditTime: 2019-06-28 13:41:34
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api import DBMysql
from parse_data import RepairMysqlData
import sys
import pandas as pd
from params import *
import time



# 创建repair_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, db=M_DATABASE)
sql = db.sql_for_create(tablename= M_N_TABLENAME,columns=M_COLUMNS)
db.sql_execute(sql)

# repair_table
rmd = RepairMysqlData(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
rmd.get_table_id(M_N_TABLENAME, M_O_TABLENAME)
while rmd.new_tableid < rmd.old_tableid:
    #获取未修复数据
    non_repair_data = rmd.get_non_repair_data(old_tablename=M_O_TABLENAME, columns=M_COLUMNS, n=100)
    #修复数据
    repaired = rmd.repair_multiple_rows(non_repair_data)
    # print(repaired)
    #插入新表
    sql = db.sql_for_insert(tablename=M_N_TABLENAME,columns=M_COLUMNS, values=repaired)
    db.sql_execute(sql)


# # 创建resolve_table
# db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, db=M_DATABASE)
# sql = db.sql_for_create(tablename= M_R_TABLENAME,columns=M_R_COLUMNS)
# db.sql_execute(sql)

# # resolve_table

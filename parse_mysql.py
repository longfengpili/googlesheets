'''
@Author: longfengpili
@Date: 2019-06-27 12:26:40
@LastEditTime: 2019-06-27 19:56:18
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from db_api.db_api import DBMysql
from parse_data.repair_mysql_data import RepairMysqlData
import sys
import pandas as pd
from params import *
import time



# 创建repair_table
db = DBMysql(host=M_HOST, user=M_USER, password=M_PASSWORD, db=M_DATABASE)
sql = db.sql_for_create(tablename= M_N_TABLENAME,columns=M_COLUMNS)
db.sql_execute(sql)

# repair_table
repair_mysql_data = RepairMysqlData()
repair_mysql_data.get_table_id()
# repair_mysql_data.old_table_id = 10000
while repair_mysql_data.repair_table_id < repair_mysql_data.old_table_id:
    # repair_mysql_data.repair_table_id = 5
    #获取未修复数据
    non_repair_data = repair_mysql_data.get_non_repair_data(n=100)
    #修复数据
    repaired = repair_mysql_data.repair_multiple_rows(non_repair_data)
    # print(repaired)
    #插入新表
    sql = db.sql_for_insert(tablename=M_N_TABLENAME,columns=M_COLUMNS, values=repaired)
    db.sql_execute(sql)
    # time.sleep(2)

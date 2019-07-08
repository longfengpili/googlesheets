'''
@Author: longfengpili
@Date: 2019-07-01 11:59:54
@LastEditTime: 2019-07-08 13:57:23
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from psetting import *
from db_api import DBMysql, DBRedshift
from parse_data import RepairMysqlDataToRedshift, ResolveRedshiftData
import sys
from sendmessage import sent_message_tousers
from datetime import datetime

import argparse
parser = argparse.ArgumentParser(description='input id_min id_max')
parser.add_argument('-id_min', type=int, default=None)
parser.add_argument('-id_max', type=int, default=None)
args = parser.parse_args()
id_min = args.id_min
id_max = args.id_max


R_REPAIR_TABLENAME = R_REPAIR_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表
R_RESOLVE_TABLENAME = R_RESOLVE_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表

# 创建repair_table
db = DBRedshift(host=R_HOST, user=R_USER,
                password=R_PASSWORD, database=R_DATABASE)
sql = db.sql_for_create(tablename=R_REPAIR_TABLENAME, columns=M_ORIGINAL_COLUMNS)
db.sql_execute(sql)
rmdtr = RepairMysqlDataToRedshift(mysql_host=M_HOST, mysql_user=M_USER, mysql_password=M_PASSWORD, mysql_database=M_DATABASE,
                                    redshift_host=R_HOST, redshift_user=R_USER, redshift_password=R_PASSWORD, redshift_database=R_DATABASE,
                                    orignal_columns=M_ORIGINAL_COLUMNS)
rmdtr.repair_mysql_main(orignal_tablename=M_ORIGINAL_TABLENAME,
                        repair_tablename=R_REPAIR_TABLENAME, id_min=id_min, id_max=id_max)

# 创建resolve_table
db = DBRedshift(host=R_HOST, user=R_USER,
                password=R_PASSWORD, database=R_DATABASE)
sql = db.sql_for_create(tablename= R_RESOLVE_TABLENAME,columns=R_RESOLVE_COLUMNS)
db.sql_execute(sql)

rsrd = ResolveRedshiftData(host=R_HOST, user=R_USER,
                       password=R_PASSWORD, database=R_DATABASE, orignal_columns=M_ORIGINAL_COLUMNS, resolve_columns=R_RESOLVE_COLUMNS)
rsrd.resolve_redshift_main(repair_tablename=R_REPAIR_TABLENAME,
                        resolve_tablename=R_RESOLVE_TABLENAME, id_min=id_min, id_max=id_max)
                        

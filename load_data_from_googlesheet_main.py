'''
@Author: longfengpili
@Date: 2019-06-19 15:39:37
@LastEditTime: 2019-06-27 14:29:38
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from googlesheets.get_spreadsheet import GetSpreadsheet
from db_api.db_api import DBRedshift
import sys
import pandas as pd
from params import *

spreadsheet_id = spreadsheet_id

# try:
#     spreadsheet_id = sys.argv[1]
# except:
#     spreadsheet_id = input('please input your spredsheet_id:')

table_name = R_TABLENAME
columns = R_COLUMNS

c = GetSpreadsheet()
values = c.get_spreadsheet_main(spreadsheet_id=spreadsheet_id, columns=columns)

db = DBRedshift(host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE)

sql = db.sql_for_drop(table_name)
result = db.sql_execute(sql)
print(result)

sql1 = db.sql_for_create(tablename=table_name, columns=columns)
result = db.sql_execute(sql1)
print(result)

sql2 = db.sql_for_insert(tablename=table_name, columns=columns, values=values[1:])
result = db.sql_execute(sql2)
print(result)

sql3 = db.sql_for_select(tablename=table_name, columns=columns)
result = db.sql_execute(sql3,count=10)
print(pd.DataFrame(result,columns=values[0]))

'''
@Author: longfengpili
@Date: 2019-08-12 17:57:20
@LastEditTime: 2019-08-12 17:57:20
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python
# -*- coding:utf-8 -*-



from db_api import DBRedshift
from psetting import *

R_RESOLVE_TABLENAME = R_RESOLVE_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表
R_AD_RESOLVE_TABLENAME = R_AD_RESOLVE_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表

dbr = DBRedshift(host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE)

tablename = input(f'''
please choice tablename !!!!
1. {R_RESOLVE_TABLENAME}
2. {R_AD_RESOLVE_TABLENAME}

you want alter columns :''')


if tablename == '1':
    dbr.alter_table_columns(tablename=R_RESOLVE_TABLENAME, columns=R_RESOLVE_COLUMNS)
elif tablename == '2':
    dbr.alter_table_columns(tablename=R_AD_RESOLVE_TABLENAME, columns=R_AD_RESOLVE_COLUMNS)
else:
    print('do not alter anytable !')


'''
@Author: longfengpili
@Date: 2019-06-19 15:34:00
@LastEditTime: 2019-07-01 18:55:01
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
''' 


import sys
sys.path.append('..')
import mysetting


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# GOOGLE
CREDENTIALS_JSON_PATH = '../credentials.json'
CREDENTIALS_PICKLE_PATH = '../token.pickle'
# Redshift
R_DATABASE = mysetting.redshift_database
R_HOST = mysetting.redshift_host
R_USER = mysetting.redshift_user
R_PASSWORD = mysetting.redshift_password
R_TABLENAME = mysetting.item_table_name
R_COLUMNS = mysetting.item_columns
spreadsheet_id = mysetting.spreadsheet_id
# Mysql
M_DATABASE = mysetting.mysql_database
M_HOST = mysetting.mysql_j_host
M_USER = mysetting.mysql_user
M_PASSWORD = mysetting.mysql_j_password
M_ORIGINAL_TABLENAME = mysetting.original_table
M_ORIGINAL_COLUMNS = mysetting.original_columns
M_REPAIR_TABLENAME = mysetting.repair_table
M_RESOLVE_TABLENAME = mysetting.resolve_table
M_RESOLVE_COLUMNS = mysetting.resolve_columns
# daily
EXECUTE_ORDER = mysetting.execute_order



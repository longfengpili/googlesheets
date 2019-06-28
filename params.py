
'''
@Author: longfengpili
@Date: 2019-06-19 15:34:00
@LastEditTime: 2019-06-28 12:27:42
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
M_HOST = mysetting.mysql_host
M_USER = mysetting.mysql_user
M_PASSWORD = mysetting.mysql_password
M_O_TABLENAME = mysetting.events_table
M_COLUMNS = mysetting.events_columns
M_N_TABLENAME = mysetting.repair_table
M_R_TABLENAME = mysetting.resolve_table
M_R_COLUMNS = mysetting.resolve_columns



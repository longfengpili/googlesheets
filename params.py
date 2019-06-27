
'''
@Author: longfengpili
@Date: 2019-06-19 15:34:00
@LastEditTime: 2019-06-27 11:36:44
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

CREDENTIALS_JSON_PATH = '../credentials.json'
CREDENTIALS_PICKLE_PATH = '../token.pickle'

DATABASE = mysetting.database
HOST = mysetting.host
USER = mysetting.user
PASSWORD = mysetting.password

TABLENAME = mysetting.table_name
COLUMNS = mysetting.columns

spreadsheet_id = mysetting.spreadsheet_id

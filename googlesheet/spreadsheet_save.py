'''
@Author: longfengpili
@Date: 2019-07-02 11:41:25
@LastEditTime: 2019-07-02 12:09:58
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from googlesheet import Spreadsheet
from db_api import DBMysql
import sys
from psetting import *

class SaveSpreadSheet(Spreadsheet):
    def __init__(self, host, user, password, database, tablename, columns, spreadsheet_id):
        self.db = None
        self.conn = None
        self.host = host
        self.port = 3306
        self.user = user
        self.password = password
        self.database = database
        self.tablename = tablename
        self.columns = columns
        self.creds_pickle_path = CREDENTIALS_PICKLE_PATH
        self.creds_json_path = CREDENTIALS_JSON_PATH
        self.scopes = SCOPES
        self.spreadsheet_id = spreadsheet_id

    def _get_spreadsheet_value(self, sheetname):
        values = self.get_spreadsheet_main(self.spreadsheet_id, sheetname=sheetname, columns=self.columns)
        return values

    def _mysql_connect(self):
        if not self.db:
            self.db = DBMysql(host=self.host, user=self.user,
                              password=self.password, database=self.database)
        if not self.conn:
            self.conn = self.db.connect()

    def save_values(self, sheetname):
        values = self._get_spreadsheet_value(sheetname)
        self._mysql_connect()
        sql = self.db.sql_for_drop(self.tablename)
        self.db.sql_execute(sql)
        sql = self.db.sql_for_create(tablename=self.tablename, columns=self.columns)
        self.db.sql_execute(sql)

        
        




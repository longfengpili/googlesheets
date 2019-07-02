'''
@Author: longfengpili
@Date: 2019-06-19 15:18:16
@LastEditTime: 2019-07-02 15:36:56
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

import pickle
import os.path
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import logging
from logging import config

config.fileConfig('parselog.conf')
spreadsheet_logger = logging.getLogger('spreadsheet')

from psetting import *


class Spreadsheet(object):

    def __init__(self):
        self.creds_pickle_path = CREDENTIALS_PICKLE_PATH
        self.creds_json_path = CREDENTIALS_JSON_PATH
        self.scopes = SCOPES

    def get_credential(self):
        creds = None
        if os.path.exists(self.creds_pickle_path):
            with open(self.creds_pickle_path, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.creds_json_path, self.scopes)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open(self.creds_pickle_path, 'wb') as token:
                pickle.dump(creds, token)
        return creds

    def get_sheet_value(self, creds, spreadsheet_id, sheetname=None, range='a:z'):
        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
        sheet = service.spreadsheets()
        if sheetname:
            range = sheetname + '!' + range
        else:
            range = range
            
        result = sheet.values().get(spreadsheetId=spreadsheet_id,range=range).execute()
        # spreadsheet_logger.info(result)
        values = result.get('values', [])

        if not values:
            return 'No data found !'
        else:
            return values

    def get_spreadsheet_main(self, spreadsheet_id,sheetname=None, columns=None):
        columns_index = {}
        result = []
        creads = self.get_credential()
        values = self.get_sheet_value(creads, spreadsheet_id=spreadsheet_id, sheetname=sheetname)
        # print(values)
        if set(columns) <= set(values[0]):#检测第一行是否有列名
            values_column = values[0]
            for column in columns:
                ix = values_column.index(column)
                lens = re.findall('\d{3,}', columns.get(column))
                lens = lens[0] if lens else 128
                columns_index[ix] = lens
            # print(columns_index)
        else:#需要按照顺序提供列名(按照顺序取数据)
            for ix, column in enumerate(columns):
                lens = re.findall('\d{3,}', columns.get(column))
                lens = lens[0] if lens else 128
                columns_index[ix] = lens
                
        for value in values:
            value_ = []
            for ix in columns_index:
                try:
                    v = (value[ix]).lower()
                    v = v[:columns_index.get(ix)]
                except:
                    v = ''
                value_.append(v)
            # print(value_)
            if value_.count('') < len(columns) // 2 and value_ not in result:
                result.append(value_)
        return result



    

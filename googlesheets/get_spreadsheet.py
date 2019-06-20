'''
@Author: longfengpili
@Date: 2019-06-19 15:18:16
@LastEditTime: 2019-06-20 12:28:02
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import config


class GetSpreadsheet(object):

    def __init__(self):
        self.creds_pickle_path = config.CREDENTIALS_PICKLE_PATH
        self.creds_json_path = config.CREDENTIALS_JSON_PATH
        self.scopes = config.SCOPES

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
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        if sheetname:
            range = sheetname + '!' + range
        else:
            range = range
            
        result = sheet.values().get(spreadsheetId=spreadsheet_id,range=range).execute()
        values = result.get('values', [])

        if not values:
            return 'No data found !'
        else:
            return values
    

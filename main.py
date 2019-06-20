'''
@Author: longfengpili
@Date: 2019-06-19 15:39:37
@LastEditTime: 2019-06-20 12:28:18
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from googlesheets.get_spreadsheet import GetSpreadsheet
import sys

try:
    spreadsheet_id = sys.argv[1]
except:
    spreadsheet_id = input('please input your spredsheet_id:')


c = GetSpreadsheet()
creads = c.get_credential()
values = c.get_sheet_value(
    creads, spreadsheet_id=spreadsheet_id)

print(values)

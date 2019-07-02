'''
@Author: longfengpili
@Date: 2019-06-19 15:39:37
@LastEditTime: 2019-07-02 15:40:37
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from googlesheet import SaveSpreadSheet
from psetting import *


def save_main(spreadsheet_id, sheetname, tablename, columns, primary_key=True):
    ss = SaveSpreadSheet(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE, spreadsheet_id=spreadsheet_id)
    ss.save_values(sheetname=sheetname, tablename=tablename, columns=columns, primary_key=primary_key)



load = input(f'''
please choice reload data !!!!
1. reload item info
2. reload bi table
3. reload new user funnel

you want run :''')

if load == '1':
    save_main(spreadsheet_id=ITEM_SPREADSHEET_ID, sheetname=ITME_SHEETNAME, tablename=ITEM_TABLENAME, columns=ITEM_COLUMNS)
elif load == '2':
    save_main(spreadsheet_id=BI_SPREADSHEET_ID, sheetname=BI_SHEETNAME, tablename=BI_TABLENAME, columns=BI_COLUMNS, primary_key=False)
elif load == '3':
    save_main(spreadsheet_id=BI_SPREADSHEET_ID, sheetname=BI_SHEETNAME, tablename=BI_TABLENAME, columns=BI_COLUMNS, primary_key=True)



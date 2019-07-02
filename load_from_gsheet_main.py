'''
@Author: longfengpili
@Date: 2019-06-19 15:39:37
@LastEditTime: 2019-07-02 14:17:04
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from googlesheet import SaveSpreadSheet
from psetting import *

def save_main(spreadsheet_id, sheetname, tablname, columns):
    ss = SaveSpreadSheet(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE, spreadsheet_id=spreadsheet_id)
    ss.save_values(sheetname=sheetname, tablename=tablname, columns=columns)



load = input(f'''
please choice reload data !!!!
1. reload item info
2. reload bi table
3. reload new user funnel

you want run :''')

if load == '1':
    save_main(spreadsheet_id=ITEM_SPREADSHEET_ID, sheetname=ITME_SHEETNAME, tablname=ITEM_TABLENAME, columns=ITEM_COLUMNS)



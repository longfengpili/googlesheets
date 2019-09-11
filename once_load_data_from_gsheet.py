'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-07-10 20:15:58
@github: https://github.com/longfengpili
'''


#!/usr/bin/env python
# -*- coding:utf-8 -*-

from googlesheet import SaveSpreadSheetToRedshift
from psetting import *


def save_main(spreadsheet_id, sheetname, tablename, columns, primary_key=True):
    ss = SaveSpreadSheetToRedshift(
        host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE, spreadsheet_id=spreadsheet_id)
    ss.save_values(sheetname=sheetname, tablename=tablename, columns=columns, primary_key=primary_key)



load = input(f'''
please choice reload data !!!!
1. reload item info
2. reload bi table
3. reload new user funnel (0.0.16)
4. reload user load funnel
5. reload new user funnel (0.0.21)
6. reload new user funnel (new0902)

you want run :''')

if load == '1':
    save_main(spreadsheet_id=ITEM_SPREADSHEET_ID, sheetname=ITME_SHEETNAME, tablename=R_ITEM_TABLENAME, columns=ITEM_COLUMNS)
elif load == '2':
    save_main(spreadsheet_id=BI_SPREADSHEET_ID, sheetname=BI_SHEETNAME, tablename=R_BI_TABLENAME, columns=BI_COLUMNS, primary_key=False)
elif load == '3':
    save_main(spreadsheet_id=FUNNEL_SPREADSHEET_ID, sheetname=FUNNEL_SHEETNAME, tablename=R_FUNNEL_TABLENAME, columns=FUNNEL_COLUMNS, primary_key=True)
elif load == '4':
    save_main(spreadsheet_id=FUNNEL_SPREADSHEET_ID, sheetname=LOAD_FUNNEL_SHEETNAME, tablename=R_LOAD_FUNNEL_TABLENAME, columns=LOAD_FUNNEL_COLUMNS, primary_key=True)
elif load == '5':
    save_main(spreadsheet_id=FUNNEL_SPREADSHEET_ID, sheetname=FUNNEL_SHEETNAME_NEW, tablename=R_FUNNEL_TABLENAME_NEW, columns=FUNNEL_COLUMNS_NEW, primary_key=True)
elif load == '6':
    save_main(spreadsheet_id=FUNNEL_SPREADSHEET_ID, sheetname=FUNNEL_SHEETNAME_NEW0902, tablename=R_FUNNEL_TABLENAME_NEW0902, columns=FUNNEL_COLUMNS_NEW0902, primary_key=True)


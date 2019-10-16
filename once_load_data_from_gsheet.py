'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-10-16 15:07:26
@github: https://github.com/longfengpili
'''


#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from googlesheet import SaveSpreadSheetToRedshift
from psetting import *


def save_main(spreadsheet_id, sheetname, tablename, columns):
    ss = SaveSpreadSheetToRedshift(
        host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE, spreadsheet_id=spreadsheet_id)
    ss.save_values(sheetname=sheetname, tablename=tablename, columns=columns)



load = input(f'''
please choice reload data !!!!
1. reload item info
2. reload bi table
3. reload new user funnel (1018)
4. reload quest (0926)

you want run :''')

if load == '1':
    save_main(spreadsheet_id=ITEM_SPREADSHEET_ID, sheetname=ITME_SHEETNAME, tablename=R_ITEM_TABLENAME, columns=ITEM_COLUMNS)
elif load == '2':
    save_main(spreadsheet_id=BI_SPREADSHEET_ID, sheetname=BI_SHEETNAME, tablename=R_BI_TABLENAME, columns=BI_COLUMNS)
elif load == '3':
    save_main(spreadsheet_id=FUNNEL_SPREADSHEET_ID, sheetname=FUNNEL_SHEETNAME, tablename=R_FUNNEL_TABLENAME, columns=FUNNEL_COLUMNS)
elif load == '4':
    save_main(spreadsheet_id=QUEST_SPREADSHEET_ID, sheetname=QUEST_SHEETNAME, tablename=R_QUEST_TABLENAME, columns=QUEST_COLUMNS)



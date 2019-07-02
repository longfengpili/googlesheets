'''
@Author: longfengpili
@Date: 2019-06-19 15:39:37
@LastEditTime: 2019-07-02 12:10:22
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from googlesheet import SaveSpreadSheet
from psetting import *

ss = SaveSpreadSheet(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE, tablename=ITEM_TABLENAME, columns=ITEM_COLUMNS, spreadsheet_id=ITEM_SPREADSHEET_ID)
values = ss._get_spreadsheet_value(sheetname='GameElement')
# print(values)
ss.save_values(sheetname='GameElement')

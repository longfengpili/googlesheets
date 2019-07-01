'''
@Author: longfengpili
@Date: 2019-07-01 14:11:55
@LastEditTime: 2019-07-01 17:27:23
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

# from daily_work import ParseSql

# ps = ParseSql()
# files = ps.get_sql_files()
# sqls = ps.get_files_sqls(date='2019-07-01', date_str='2019-06-30')
# print(sqls)

from daily_work import DailyMain
from psetting import *


dm = DailyMain(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
dm.daily_execute(execute_order=EXECUTE_ORDER,date='2019-07-01', date_str='2019-06-30')

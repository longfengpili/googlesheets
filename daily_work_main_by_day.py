'''
@Author: longfengpili
@Date: 2019-07-01 14:11:55
@LastEditTime: 2019-07-02 11:29:51
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''


from daily_work import DailyMain
from psetting import *
from datetime import datetime, date, timedelta

today = date.today()

dm = DailyMain(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
dm.daily_execute(execute_order=EXECUTE_ORDER,date=today,date_str=today)

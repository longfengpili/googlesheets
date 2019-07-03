'''
@Author: longfengpili
@Date: 2019-07-01 14:11:55
@LastEditTime: 2019-07-03 17:52:45
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from daily_work import DailyMainRedshift
from psetting import *
from datetime import datetime, date, timedelta

import sys
params = sys.argv
params_execute = ' '.join(params[1:])


def set_date(interval_day):
    today = date.today()
    set_date = today + timedelta(days=int(interval_day))
    set_date = set_date.strftime('%Y-%m-%d')
    return set_date

def daily_work_main(date_min, date_max, **kw):
    dm = DailyMainRedshift(host=R_HOST, user=R_USER,
                           password=R_PASSWORD, database=R_DATABASE)
    dm.daily_execute_all(execute_order=EXECUTE_ORDER, date_min=date_min, date_max=date_max, **kw)


def daily_work_single_main(schema, date_min, date_max, **kw):
    dm = DailyMainRedshift(host=R_HOST, user=R_USER,
                           password=R_PASSWORD, database=R_DATABASE)
    dm.daily_execute_single(schema=schema, date_min=date_min, date_max=date_max, **kw)


# 格式："\033[字背景颜色；字体颜色m————————\033[0m"   (——————表示字符串)
if not params_execute:
    params_execute = input(f'''every params please add blank !
【PARAM_1】which sqlfile?
    A.all
    B.raw_data
    C.fact_data
    D.report_data
【PARAM_2】from begin days? 
    example:today is 0, yesterday is -1
【PARAM_3】to end days?
    example:today is 0, yesterday is -1
请选择要执行的内容：''')


p1, p2, p3 = params_execute.split(' ')
print(p1,p2,p3)
if p1.upper() == 'A':
    daily_work_main(set_date(p2), set_date(p3))
elif p1.upper() == 'B':
    daily_work_single_main('raw_data', set_date(p2), set_date(p3))
elif p1.upper() == 'C':
    daily_work_single_main('fact_data', set_date(p2), set_date(p3))
elif p1.upper() == 'D':
    daily_work_single_main('report_data', set_date(p2), set_date(p3))



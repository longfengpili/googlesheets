'''
@Author: longfengpili
@Date: 2019-07-01 14:11:55
@LastEditTime: 2019-07-02 16:46:20
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''


from daily_work import DailyMain
from psetting import *
from datetime import datetime, date, timedelta


def set_date(interval_day):
    today = date.today()
    set_date = today - timedelta(days=interval_day)
    set_date = set_date.strftime('%Y-%m-%d')
    return set_date

def daily_work_main(date, **kw):
    dm = DailyMain(host=M_HOST, user=M_USER, password=M_PASSWORD, database=M_DATABASE)
    dm.daily_execute_all(execute_order=EXECUTE_ORDER, date=date, **kw)


def daily_work_single_main(schema, date, **kw):
    dm = DailyMain(host=M_HOST, user=M_USER,password=M_PASSWORD, database=M_DATABASE)
    dm.daily_execute_single(schema=schema, date=date, **kw)


date_execute = input(f'''
1. all          【{set_date(0)}】
2. all          【{set_date(1)}】
3. all          【{set_date(2)}】
4. raw_data     【{set_date(0)}】
5. raw_data     【{set_date(1)}】
6. raw_data     【{set_date(2)}】
7. fact_data    【{set_date(0)}】
8. fact_data    【{set_date(1)}】
9. fact_data    【{set_date(2)}】
10. report_data 【{set_date(0)}】
11. report_data 【{set_date(1)}】
12. report_data 【{set_date(2)}】
请选择要执行的内容：''')

if date_execute == '1' or not date_execute:
    daily_work_main(set_date(0))
elif date_execute == '2':
    daily_work_main(set_date(1))
elif date_execute == '3':
    daily_work_main(set_date(2))
elif date_execute == '4':
    daily_work_single_main('raw_data', set_date(0))
elif date_execute == '5':
    daily_work_single_main('raw_data', set_date(1))
elif date_execute == '6':
    daily_work_single_main('raw_data', set_date(2))
elif date_execute == '4':
    daily_work_single_main('fact_data', set_date(0))
elif date_execute == '5':
    daily_work_single_main('fact_data', set_date(1))
elif date_execute == '6':
    daily_work_single_main('fact_data', set_date(2))
elif date_execute == '4':
    daily_work_single_main('report_data', set_date(0))
elif date_execute == '5':
    daily_work_single_main('report_data', set_date(1))
elif date_execute == '6':
    daily_work_single_main('report_data', set_date(2))


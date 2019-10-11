'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-10-11 17:02:19
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from db_api import DBRedshift
from psetting import *


db1 = DBRedshift(host=SDK_REDSHIFT_HOST, user=SDK_REDSHIFT_USER, password=SDK_REDSHIFT_PASSWORD, database=SDK_REDSHIFT_DATABASE)
sql = '''select distinct fpid,
upper(first_value(country ignore nulls) over (partition by fpid order by created_at
rows between unbounded preceding and unbounded following)) as country
from animatch_bi.event_adjust_callback
where fpid is not null
;'''
count, result = db1.sql_execute(sql)

db2 = DBRedshift(host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE)
tablename = 'fact_data_aniland.sdk_country'
columns={'fpid': 'varchar', 'country':'varchar'}
db2.drop_table(tablename)
db2.create_table(tablename, columns=columns, index=['fpid'])
sql = db2.sql_for_insert(tablename, columns, result)
db2.sql_execute(sql)

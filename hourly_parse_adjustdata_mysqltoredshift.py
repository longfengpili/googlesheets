'''
@Author: longfengpili
@Date: 2019-07-15 12:03:41
@LastEditTime: 2019-10-16 10:45:07
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-



from psetting import *
from db_api import DBMysql, DBRedshift
from parse_bi_data import CopyDataOVO, ResolveData
import sys
from datetime import datetime

import argparse
parser = argparse.ArgumentParser(description='input id_min id_max')
parser.add_argument('-id_min', type=int, default=None)
parser.add_argument('-id_max', type=int, default=None)
parser.add_argument('-execute', type=str, default=None)
args = parser.parse_args()
id_min = args.id_min
id_max = args.id_max
execute = args.execute

R_AD_REPAIR_TABLENAME = R_AD_REPAIR_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表
R_AD_RESOLVE_TABLENAME = R_AD_RESOLVE_TABLENAME + '_' + M_HOST.split('.')[-1]  # 根据不同的数据库创建不同的表

if id_max:
    print('id_max do not effect, becouse the original_tablename have no id !')


def copy_adjust_data(id_min, id_max):
    rmdovo = CopyDataOVO(db_type='mysql', db_host=M_HOST, db_user=M_USER, db_password=M_PASSWORD, db_database=M_DATABASE,
                                db2_type='redshift', db2_host=R_HOST, db2_user=R_USER, db2_password=R_PASSWORD, db2_database=R_DATABASE,
                                original_columns=M_AD_ORIGINAL_COLUMNS)
    rmdovo.copy_adjust_data_main(original_tablename=M_AD_ORIGINAL_TABLENAME,
                                repair_tablename=R_AD_REPAIR_TABLENAME, id_min=id_min, id_max=id_max, suffix='idxu')

def resolve_data(id_min, id_max):
    rd = ResolveData(host=R_HOST, user=R_USER, password=R_PASSWORD, database=R_DATABASE, 
                     original_columns=M_AD_ORIGINAL_COLUMNS, resolve_columns=R_AD_RESOLVE_COLUMNS, resolve_index=R_AD_RESOLVE_INDEX,
                     no_resolve_columns=R_AD_NO_RESOLVE_COLUMNS, db_type='redshift')
    rd.resolve_data_main(repair_tablename=R_AD_REPAIR_TABLENAME, resolve_tablename=R_AD_RESOLVE_TABLENAME, id_min=id_min, id_max=id_max)
                        
if execute == 'repair':
    copy_adjust_data(id_min, id_max)
elif execute == 'resolve':
    resolve_data(id_min, id_max)
else:
    copy_adjust_data(id_min, id_max)
    resolve_data(id_min, id_max)
    

'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-10-12 12:57:01
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-


from psetting import *
from parse_bi_data import CopyDataOVO

import logging
from logging import config

config.fileConfig('parselog.conf')
bakeup_logger = logging.getLogger('bakeup')

import argparse
parser = argparse.ArgumentParser(description='input id_min id_max')
parser.add_argument('-id_min', type=int, default=None)
parser.add_argument('-id_max', type=int, default=None)
args = parser.parse_args()
id_min = args.id_min
id_max = args.id_max

M_BUKEUP_GAME_TABLENAME = 'bidatanew'
M_BUKEUP_GAME_COLUMNS = M_BUKEUP_TABLE[M_BUKEUP_GAME_TABLENAME]

for tablename, columns in M_BUKEUP_TABLE.items():
    bakeup_logger.info(f'\n' + f"开始备份{tablename}".center(80, '='))
    to_tablename = tablename + '_bakeup' + f"{M_BUKEUP_FROM_HOST.replace('.', '_')}"
    rmdovo = CopyDataOVO(db_type='mysql', db_host=M_BUKEUP_FROM_HOST, db_user=M_BUKEUP_USER, db_password=M_BUKEUP_FROM_PASSWORD, db_database=M_BUKEUP_DATABASE,
                                db2_type='mysql', db2_host=M_BUKEUP_TO_HOST, db2_user=M_BUKEUP_USER, db2_password=M_BUKEUP_TO_PASSWORD, db2_database=M_BUKEUP_DATABASE,
                                original_columns=columns)
    rmdovo.copy_game_data_main(original_tablename=tablename, repair_tablename=to_tablename, id_min=id_min, id_max=id_max, is_repair=False)
    bakeup_logger.info(f'\n' + f"备份{tablename}结束！".center(80, '='))

                   

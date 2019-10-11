'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-10-11 20:43:22
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-


from psetting import *
from parse_bi_data import CopyDataOVO

import argparse
parser = argparse.ArgumentParser(description='input id_min id_max')
parser.add_argument('-id_min', type=int, default=None)
parser.add_argument('-id_max', type=int, default=None)
args = parser.parse_args()
id_min = args.id_min
id_max = args.id_max

NEW_M_BUKEUP_GAME_TABLENAME = M_BUKEUP_GAME_TABLENAME + '_bakeup' + f"{M_HOST.replace('.', '_')}"
NEW_M_BUKEUP_ADJUST_TABLENAME = M_BUKEUP_ADJUST_TABLENAME + '_bakeup' + f"{M_HOST.replace('.', '_')}"


rmdovo = CopyDataOVO(db_type='mysql', db_host=M_HOST, db_user=M_USER, db_password=M_PASSWORD, db_database=M_DATABASE,
                            db2_type='mysql', db2_host=M_BUKEUP_HOST, db2_user=M_BUKEUP_USER, db2_password=M_BUKEUP_PASSWORD, db2_database=M_BUKEUP_DATABASE,
                            original_columns=M_BUKEUP_GAME_COLUMNS)
rmdovo.copy_game_data_main(original_tablename=M_BUKEUP_GAME_TABLENAME,repair_tablename=NEW_M_BUKEUP_GAME_TABLENAME, id_min=id_min, id_max=id_max, is_repair=False)

# rmdovo = CopyDataOVO(db_type='mysql', db_host=M_HOST, db_user=M_USER, db_password=M_PASSWORD, db_database=M_DATABASE,
#                             db2_type='mysql', db2_host=M_BUKEUP_HOST, db2_user=M_BUKEUP_USER, db2_password=M_BUKEUP_PASSWORD, db2_database=M_BUKEUP_DATABASE,
#                             original_columns=M_BUKEUP_ADJUST_COLUMNS)
# rmdovo.copy_game_data_main(original_tablename=M_BUKEUP_GAME_TABLENAME,repair_tablename=NEW_M_BUKEUP_ADJUST_TABLENAME, id_min=id_min, id_max=id_max, is_repair=False)                       

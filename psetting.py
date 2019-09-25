'''
@Author: longfengpili
@Date: 2019-07-10 20:15:58
@LastEditTime: 2019-09-25 14:50:28
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-




import sys
sys.path.append('..')
import mysetting


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# GOOGLE
CREDENTIALS_JSON_PATH = '../credentials.json'
CREDENTIALS_PICKLE_PATH = '../token.pickle'
#item_info
ITEM_TABLENAME = mysetting.item_table
R_ITEM_TABLENAME = mysetting.r_item_table
ITEM_COLUMNS = mysetting.item_columns
ITEM_SPREADSHEET_ID = mysetting.item_spreadsheet_id
ITME_SHEETNAME = mysetting.item_sheetname
#bi_info
BI_TABLENAME = mysetting.bi_table
R_BI_TABLENAME = mysetting.r_bi_table
BI_COLUMNS = mysetting.bi_columns
BI_SPREADSHEET_ID = mysetting.bi_spreadsheet_id
BI_SHEETNAME = mysetting.bi_sheetname
#funnel_info
FUNNEL_TABLENAME = mysetting.funnel_table
R_FUNNEL_TABLENAME = mysetting.r_funnel_table
FUNNEL_COLUMNS = mysetting.funnel_columns
FUNNEL_SPREADSHEET_ID = mysetting.funnel_spreadsheet_id
FUNNEL_SHEETNAME = mysetting.funnel_sheetname
#funnel_info_new
FUNNEL_TABLENAME_NEW = mysetting.funnel_table_new
R_FUNNEL_TABLENAME_NEW = mysetting.r_funnel_table_new
FUNNEL_COLUMNS_NEW = mysetting.funnel_columns
FUNNEL_SPREADSHEET_ID = mysetting.funnel_spreadsheet_id
FUNNEL_SHEETNAME_NEW = mysetting.funnel_sheetname_new
#funnel_info_new0902
FUNNEL_TABLENAME_NEW0902 = mysetting.funnel_table_new0902
R_FUNNEL_TABLENAME_NEW0902 = mysetting.r_funnel_table_new0902
FUNNEL_COLUMNS_NEW0902 = mysetting.funnel_columns
FUNNEL_SPREADSHEET_ID = mysetting.funnel_spreadsheet_id
FUNNEL_SHEETNAME_NEW0902 = mysetting.funnel_sheetname_new0902
#load_funnel_info
LOAD_FUNNEL_TABLENAME = mysetting.load_funnel_table
R_LOAD_FUNNEL_TABLENAME = mysetting.r_load_funnel_table
LOAD_FUNNEL_COLUMNS = mysetting.load_funnel_columns
FUNNEL_SPREADSHEET_ID = mysetting.funnel_spreadsheet_id
LOAD_FUNNEL_SHEETNAME = mysetting.load_funnel_sheetname

# Redshift
R_DATABASE = mysetting.redshift_database
R_HOST = mysetting.redshift_host
R_USER = mysetting.redshift_user
R_PASSWORD = mysetting.redshift_password
R_REPAIR_TABLENAME = mysetting.r_repair_table
R_RESOLVE_TABLENAME = mysetting.r_resolve_table
R_RESOLVE_COLUMNS = mysetting.resolve_columns
R_NO_RESOLVE_COLUMNS = mysetting.no_resolve_columns
# Redshfit adjust
R_AD_REPAIR_TABLENAME = mysetting.adjust_r_repair_table
R_AD_RESOLVE_TABLENAME = mysetting.adjust_r_resolve_table
R_AD_RESOLVE_COLUMNS = mysetting.adjust_resolve_columns
R_AD_NO_RESOLVE_COLUMNS = mysetting.adjust_no_resolve_columns

# Mysql
M_DATABASE = mysetting.mysql_database
M_HOST = mysetting.mysql_host
M_USER = mysetting.mysql_user
M_PASSWORD = mysetting.mysql_password
M_ORIGINAL_TABLENAME = mysetting.original_table
M_ORIGINAL_COLUMNS = mysetting.original_columns
M_REPAIR_TABLENAME = mysetting.repair_table
M_RESOLVE_TABLENAME = mysetting.resolve_table
M_RESOLVE_COLUMNS = mysetting.resolve_columns
M_NO_RESOLVE_COLUMNS = mysetting.no_resolve_columns
# Mysql adjust
M_AD_ORIGINAL_TABLENAME = mysetting.adjust_original_table
M_AD_ORIGINAL_COLUMNS = mysetting.adjust_original_columns
M_AD_REPAIR_TABLENAME = mysetting.adjust_repair_table
M_AD_RESOLVE_TABLENAME = mysetting.adjust_resolve_table
M_AD_RESOLVE_COLUMNS = mysetting.adjust_resolve_columns
M_AD_NO_RESOLVE_COLUMNS = mysetting.adjust_no_resolve_columns

# daily
EXECUTE_ORDER = mysetting.execute_order
SQL_PATH = mysetting.sql_path
#messge
MSG_USERS = mysetting.message_users


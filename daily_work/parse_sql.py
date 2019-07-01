'''
@Author: longfengpili
@Date: 2019-07-01 14:17:41
@LastEditTime: 2019-07-01 17:24:31
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
import os
import re
import logging
from logging import config

config.fileConfig('parselog.conf')
pslogger = logging.getLogger('parsesql')

class ParseSql(object):
    def __init__(self, sql_path='./sql/'):
        self.sql_path = sql_path

    def get_sql_files(self):
        files = os.listdir(self.sql_path)
        files = [self.sql_path + file for file in files]
        return files
        
    def get_sqls_params(self, filename):
        with open(filename, 'r', encoding='utf-8') as f:
            sqls_txt = f.read()
        sqls = re.findall("'''\n--【(.*?)】(.*?)'''", sqls_txt, re.S)
        params = re.findall("\$(.*?)[ |\n]", sqls_txt)
        return params, sqls

    def get_file_sqls(self,filename, **kw):
        params, sqls = self.get_sqls_params(filename)
        for param in params:
            if param not in kw:
                pslogger.error(f'"{param}" need setting !')
                raise f'param need setting !'
        sqls_n = []
        for sql in sqls:
            sql_n = sql[1]
            for param in params:
                sql_n = re.sub(f"\${param}[ |\n]", f"'{kw.get(param)}'\n", sql_n)
            sql = [sql[0],sql_n]
            sqls_n.append(sql)
        return sqls_n
    
    def get_files_sqls(self, **kw):
        sqls = {}
        files = self.get_sql_files()
        for file in files:
            sqls_ = self.get_file_sqls(file,**kw)
            sqls[file] = sqls_
        
        return sqls


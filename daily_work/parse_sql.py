'''
@Author: longfengpili
@Date: 2019-07-01 14:17:41
@LastEditTime: 2019-07-03 19:15:41
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
    def __init__(self, sqlpath='./sql/'):
        self.sqlpath = sqlpath

    def get_sql_files(self):
        '''
        获取sql文件列表
        '''
        files = os.listdir(self.sqlpath)
        files = [self.sqlpath + file for file in files]
        return files
        
    def get_sqls_params(self, filename):
        '''
        @description: 获取文件中sql的参数
        @param {type} 
            filename:文件名
        @return: 
            params：文件中需要设置的参数
            sqls:文件中的sql列表
        '''
        with open(filename, 'r', encoding='utf-8') as f:
            sqls_txt = f.read()
        sqls = re.findall("```\n--【(.*?)】(.*?)```", sqls_txt, re.S)
        params = re.findall("\$(.*?)[ |\n|)]", sqls_txt)
        return params, sqls

    def get_file_sqls(self,filename, **kw):
        '''
        @description:填充sql中的参数 
        @param {type} 
            filename:文件名
            kw:sql中需要设定的参数
        @return: 
            sqls_n:修改后的sql列表
        '''
        params, sqls = self.get_sqls_params(filename)
        for param in params:
            if param not in kw:
                pslogger.error(f'【{filename}】"{param}" need setting !')
                raise f'param need setting !'
        sqls_n = []
        for sql in sqls:
            sql_n = sql[1]
            for param in params:
                sql_n = re.sub(f"\${param} ", f"'{kw.get(param)}' ", sql_n)
                sql_n = re.sub(f"\${param}\)", f"'{kw.get(param)}')", sql_n)
                sql_n = re.sub(f"\${param}\n", f"'{kw.get(param)}'\n", sql_n)
            sql = [sql[0],sql_n]
            sqls_n.append(sql)
        return sqls_n
    
    def get_files_sqls(self, **kw):
        '''所有文件中的sql'''
        sqls = {}
        files = self.get_sql_files()
        for file in files:
            sqls_ = self.get_file_sqls(file,**kw)
            sqls[file] = sqls_
        
        return sqls


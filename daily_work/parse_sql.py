'''
@Author: longfengpili
@Date: 2019-07-01 14:17:41
@LastEditTime: 2019-10-16 17:13:57
@github: https://github.com/longfengpili
'''

#!/usr/bin/env python3
#-*- coding:utf-8 -*-


import os
import re
import logging
from logging import config

config.fileConfig('parselog.conf')
pslogger = logging.getLogger('parsesql')

class ParseSql(object):
    def __init__(self, sqlpath):
        self.sqlpath = sqlpath

    def get_sql_files(self):
        '''
        获取sql文件列表
        '''
        files = os.listdir(self.sqlpath)
        files = [self.sqlpath + file for file in files]
        return files
        
    def get_file_content(self, filename):
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

        sqls_txt_ = re.sub('--.*?\n', '\n', sqls_txt) #去掉注释的代码（re.sub）
        params = re.findall("\$(\w+)[ |\n|)|;]", sqls_txt_) 
        
        sqls_find = re.findall('###\n(.*?)###', sqls_txt, re.S)
        sqls = []
        for sql in sqls_find:
            sql_des = re.search('--【(.*?)】', sql)
            sql_des = sql_des.group(1) if sql_des else 'No Description'
            sql = re.split('--【.*?】', sql)
            sql = sql[0] if len(sql) == 1 else sql[1] if len(sql) == 2 else 'error'
            sql = re.sub('--.*?\n', '\n', sql)
            sql = re.sub('\n{2,}', '\n', sql).strip()
            sql = (sql_des, sql)
            sqls.append(sql)

        # sqls = re.findall("###\n--【(.*?)】(.*?)###", sqls_txt, re.S)
        # sqls = [(sql[0], re.sub('--.*?\n', '\n' , sql[1]).strip()) for sql in sqls]
        # sqls = [(sql[0], re.sub('\n{2,}', '\n' , sql[1])) for sql in sqls]
        
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
        params_d = {}
        params, sqls = self.get_file_content(filename)
        for param in params:
            if param not in kw:
                pslogger.error(f'【{filename}】"{param}" need setting !')
                raise f'param need setting !'
        sqls_n = []
        for sql in sqls:
            sql_n = sql[1]
            for param in params:
                params_d[param] = kw.get(param)
                sql_n = re.sub(f"\${param} ", f"'{kw.get(param)}' ", sql_n)
                sql_n = re.sub(f"\${param}\)", f"'{kw.get(param)}')", sql_n)
                sql_n = re.sub(f"\${param}\n", f"'{kw.get(param)}'\n", sql_n)
                sql_n = re.sub(f"\${param};", f"'{kw.get(param)}';", sql_n)
            sql = [sql[0],sql_n]
            sqls_n.append(sql)

        if params:
            pslogger.info(f'parse sqlfile【{filename}】, params 【{len(set(params))}】 counts : {params_d}, sqls 【{len(sqls)}】 counts;')
        else:
            pslogger.info(f'parse sqlfile【{filename}】, sqls 【{len(sqls)}】 counts;')
        return sqls_n
    
    def get_files_sqls(self, **kw):
        '''所有文件中的sql'''
        sqls = {}
        files = self.get_sql_files()
        for file in files:
            sqls_ = self.get_file_sqls(file,**kw)
            sqls[file] = sqls_
        
        return sqls


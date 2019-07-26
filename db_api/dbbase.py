'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-07-23 16:27:34
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from datetime import date, timedelta, datetime
import re
import sys
import json


import logging
from logging import config

config.fileConfig('parselog.conf')
dblogger = logging.getLogger('db')


class DBBase(object):
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
    
    def connect(self):
        pass

    def __close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __check_sql_type(self, sql):
        sql = re.sub('--.*?\n', '', sql.strip())
        result = re.search('(\D.*?) ', sql.strip())
        if result:
            result = result.group(1)
        else:
            result = sql
        return result

    def __join_values(self, values):
        '''拼凑values, "、' 不同方式处理'''
        values_ = []
        for value in values:
            if re.search("'", ','.join([str(i) for i in value])):
                value = [re.sub("'", "\\'", str(i)) for i in value] #单引号问题处理 
            value_ = '(' + ','.join([f"'{i}'" for i in value]) + ')'
            values_.append(value_)
        values = ',\n'.join(values_)
        
        if re.search("\\\\'", values):
            values = re.sub("(?<!\w)\\\\'", '\"', values)
            values = re.sub("\\\\'(?!\w)", '\"', values)
            # values = re.sub("{\\\\'", '{\"', values) #开头
            # values = re.sub("\\\\': \\\\'", '\": \"', values) #中间
            # values = re.sub("\\\\', \\\\'", '\", \"', values) #中间
            # values = re.sub("\\\\'}", '\"}', values) #末尾
        return values
    
    def execute_multiple(self, cur, sql, count=None):
        change_count = 0
        sqls = sql.split(';')
        if len(sqls) > 2:
            for sql in sqls[:-1]:
                sql_type = self.__check_sql_type(sql)
                result = f'{sql_type} completed !'
                # print(result)
                if sql_type == '--':
                    pass
                elif sql == sqls[-2] and sql_type not in ['create']:
                    cur.execute(sql)
                    change_count = cur.rowcount
                    if sql_type == 'select':
                        if count:
                            result = cur.fetchmany(sql)
                        else:
                            result = cur.fetchall()
                else:
                    cur.execute(sql)
        else:
            sql_type = self.__check_sql_type(sql)
            result = f'{sql_type} completed !'
            cur.execute(sql)
            change_count = cur.rowcount
            if sql_type == 'select':
                if count:
                    result = cur.fetchmany(sql)
                else:
                    result = cur.fetchall()
        return change_count, result
        
    def sql_execute(self, sql, count=None):
        if not sql:
            return None, None
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        try:
            change_count, result = self.execute_multiple(cursor, sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            dblogger.error(e)
            dblogger.error(sql)
            sys.exit()

        self.__close()
        return change_count,result

    def sql_for_create(self, tablename, columns, primary_key=True):
        if not isinstance(columns, dict):
            raise 'colums must be a dict ! example:{"column_name":"column_type"}'

        if '.' in tablename: #redshift
            if primary_key:
                sql = f'''create table if not exists {self.database}.{tablename}
                        ({','.join([k.lower() + ' '+ f"{'varchar(128)' if v == 'varchar' else v}" for k, v in columns.items()])},
                        unique({list(columns.keys())[0]})) sortkey({list(columns.keys())[0]});'''
            else:
                sql = f'''create table if not exists {self.database}.{tablename}
                        ({','.join([k.lower() + ' '+ f"{'varchar(128)' if v == 'varchar' else v}" for k, v in columns.items()])});'''
        else:
            if primary_key:
                sql = f'''create table if not exists {self.database}.{tablename}
                        ({','.join([k.lower() + ' '+ f"{'varchar(128)' if v == 'varchar' else v}" for k, v in columns.items()])},
                        primary key ({list(columns.keys())[0]} asc)
                        ) CHARSET=utf8;'''
            else:
                sql = f'''create table if not exists {self.database}.{tablename}
                        ({','.join([k.lower() + ' '+ f"{'varchar(128)' if v == 'varchar' else v}" for k, v in columns.items()])}) CHARSET=utf8;'''

        sql = re.sub('\s{2,}', '\n', sql)
        return sql

    def sql_for_drop(self, tablename):
        sql = f'drop table if exists {self.database}.{tablename};'
        return sql

    def sql_for_insert(self, tablename, columns, values):
        sql = None
        columns = ','.join(columns)
        values = self.__join_values(values)
        if values:
            values = values.replace('"Null"', 'Null').replace("'Null'", 'Null')
            sql = f'''insert into {self.database}.{tablename}
                    ({columns})
                    values
                    {values};'''
            # dblogger.info(sql)
        return sql

    def sql_for_select(self, tablename, columns=None, contions=None):
        if columns and isinstance(columns, list):
            columns = ','.join(columns)
            
        if not columns:
            columns = '*'
            
        if contions:
            sql = f'''select {columns} from {self.database}.{tablename} where {contions};'''
        else:
            sql = f'''select {columns} from {self.database}.{tablename};'''
        return sql

    def sql_for_column_agg(self, tablename, column='*', func='count', contions=None):
        if func not in ['min', 'max', 'sum', 'count']:
            raise "func only support 'min', 'max', 'sum', 'count'"
        if contions:
            sql = f'''select {func}({column}) from {self.database}.{tablename} where {contions};'''
        else:
            sql = f'''select {func}({column}) from {self.database}.{tablename};'''
        return sql

    def sql_for_delete(self, tablename, contion):
        sql = f'''delete from {self.database}.{tablename} where {contion};'''
        return sql

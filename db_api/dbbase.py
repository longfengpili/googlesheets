'''
@Author: longfengpili
@Date: 2019-06-20 12:37:41
@LastEditTime: 2019-07-03 19:49:32
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''
from datetime import date, timedelta, datetime
import re
import sys


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
        result = re.search('(\D.*?) ', sql.strip())
        return result.group(1)

    def __join_values(self, values):
        '''拼凑values, "、' 不同方式处理'''
        values_ = ''.join([str(i) for value in values for i in value])
        search_1, search_2 = re.search("'", values_), re.search('"', values_)
        # print(search_1, search_2, values_)
        if not values_:
            pass
        elif search_1 and not search_2:
            values = [[i.replace("'", '"') for i in value] for value in values]
            values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        elif search_2 and not search_1:
            values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        elif not (search_1 and search_2):
            values = ',\n'.join(['(' + ','.join([f"'{i}'" for i in value]) + ')' for value in values])
        else:
            dblogger.error(values_)
            raise 'The values have some value use both "\'" and \'"\' !' 
        return values
    
    def execute_multiple(self, cur, sql, count=None):
        sqls = sql.split(';')
        if len(sqls) > 2:
            for sql in sqls[:-1]:
                sql_type = self.__check_sql_type(sql)
                result = f'{sql_type} completed !'
                if sql == sqls[-2] and sql_type not in ['create']:
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
                        primary key ({list(columns.keys())[0]})) sortkey({list(columns.keys())[0]});'''
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

    def sql_for_select(self, tablename, columns, contions=None):
        columns = ','.join(columns)
        if contions:
            sql = f'''select {columns} from {self.database}.{tablename} where {contions};'''
        else:
            sql = f'''select {columns} from {self.database}.{tablename};'''
        return sql

    def sql_for_delete(self, tablename, contion):
        sql = f'''delete from {self.database}.{tablename} where {contion};'''
        return sql

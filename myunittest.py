'''
@Author: longfengpili
@Date: 2019-07-12 11:05:28
@LastEditTime: 2019-07-12 14:36:28
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

import unittest
from parse_bi_data import RepairJsonData
from parse_bi_data import RepairMysqlDataToRedshift


class tasktest(unittest.TestCase):

    def setUp(self):
        print('setUp...')

    def tearDown(self):
        print(f'tearDown...')

    def test_repair_main(self):
        myjson = ''
        rjd = RepairJsonData(myjson)
        myjson = rjd.repair_main()
        # print(rjd.myjson_origin)
        print(myjson)
        print(rjd.errors)

    def test_repair_row(self):
        # myrow = list((1,'﻿{"ts":"15655"","msg_type3":"end_up","isdds":false,"t":"d""}'))
        myrow = list((1,'{"ts":"15655"","msg_type3":"end_up","isdds":false,"t":"d""}'))
        rmdtr = RepairMysqlDataToRedshift('mysql_host', 'mysql_user', 'mysql_password', 'mysql_database', 'redshift_host', 
                                        'redshift_user', 'redshift_password', 'redshift_database', 'orignal_columns')
        id, myjson, errors = rmdtr.repair_row(myrow)
        # print(rjd.myjson_origin)
        # print(id)
        # print(myjson)
        # print(errors)

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()  # 创建测试套件
    suite.addTest(tasktest('test_repair_row'))
    runner = unittest.TextTestRunner()
    runner.run(suite)

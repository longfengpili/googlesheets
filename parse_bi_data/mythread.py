'''
@Author: longfengpili
@Date: 2019-07-26 11:34:31
@LastEditTime: 2019-07-26 11:34:54
@coding: 
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@github: https://github.com/longfengpili
'''

from threading import Thread

class MyThread(Thread):
    def __init__(self, func, *args, **kw):
        super().__init__()
        self.func = func
        self.args = args
        self.kw = kw
        # self.name = name

    def run(self):
        self.result = self.func(*self.args, **self.kw)

    def get_result(self):
        # Thread.join(self) # 等待线程执行完毕
        try:
            return self.result
        except Exception:
            return None

    def thread_isalive(self):
        #         print(Thread.is_alive(self))
        return Thread.is_alive(self)

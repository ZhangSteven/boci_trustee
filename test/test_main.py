# coding=utf-8
# 

import unittest2



class TestMain(unittest2.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestMain, self).__init__(*args, **kwargs)


    def test1(self):
        self.assertEqual(1, 1)
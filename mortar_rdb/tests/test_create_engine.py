# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb import create_engine
from mock import Mock
from testfixtures import Replacer, compare
from unittest import TestCase

class TestUtility(TestCase):

    def setUp(self):
        self.r = Replacer()
        self.sa_create_engine = Mock()
        self.r.replace('mortar_rdb.sa_create_engine',self.sa_create_engine)

    def tearDown(self):
        self.r.restore()
        
    def test_normal(self):
        result = create_engine('sqlite://')
        compare(result,self.sa_create_engine.return_value)
        self.sa_create_engine.assert_called_with('sqlite://')

    def test_mysql(self):
        result = create_engine('mysql://')
        compare(result,self.sa_create_engine.return_value)
        self.sa_create_engine.assert_called_with(
            'mysql://',
            encoding='utf-8',
            pool_recycle=3600,
            )

    def test_params_passed_in_take_precedence(self):
        result = create_engine('mysql://',pool_recycle=2)
        compare(result,self.sa_create_engine.return_value)
        self.sa_create_engine.assert_called_with(
            'mysql://',
            encoding='utf-8',
            pool_recycle=2,
            )

# Copyright (c) 2011-2013 Simplistix Ltd, 2015 Chris Withers
# See license.txt for license details.

from mortar_rdb import drop_tables
from mortar_rdb.controlled import Scripts, Config, Source
from mock import Mock
from sqlalchemy import (
    Table, Column, Integer, String, Text, MetaData, create_engine
    )
from sqlalchemy.engine.reflection import Inspector
from testfixtures import (
    OutputCapture, Replacer, compare, ShouldRaise
    )
from unittest import TestCase

from .base import ControlledTest, PackageTest

import os

class ScriptsMixin:
    
    failsafe = True
    
    def _check(self, s, expected, kw=None):
        kw = kw or {}
        self.r.replace('sys.argv', ['X']+s.split())
        ex = None
        try:
            with OutputCapture() as output:
               self._callable()(**kw)
        except SystemExit as ex:  # pragma: no cover
            # Catch this as the output will
            # likely tell us what the problem was
            pass
        output.compare(expected.lstrip())
        if ex is not None:  # pragma: no cover
            raise ex
        
    def _callable(self):
        return Scripts(
            self.db_url,
            self.config,
            self.failsafe,
            )

class TestCreate(ScriptsMixin, PackageTest):

    def setUp(self):
        super(TestCreate, self).setUp()
        self.db_url = 'sqlite:///'+self.dir.getpath('sqlite.db')

    def _setup_config(self):
        # setup
        metadata = MetaData()
        self.mytable = Table('user', metadata,
                             Column('id', Integer, primary_key=True),
                             )
        self.config = Config(Source(self.mytable))
        
    def _check_db(self, expected_metadata):
        actual_metadata = MetaData(bind=create_engine(self.db_url))
        actual_metadata.reflect()
        # hmm, not much of a test right now, could do with more depth
        compare(expected_metadata.tables.keys(),
                actual_metadata.tables.keys())
            
    def test_url_from_command_line(self):
        self._setup_config()        
        # make sure we're actually using the url from the command line:
        db_url = self.db_url
        self.db_url = 'junk'
        self._check('--url=%s create' % db_url, '''
For database at %s:

Creating the following tables:
user
''' % (db_url, ))
            
    def test_pass_in_argv(self):
        self._setup_config()
        # check we can pass in argv if we're being called as a sub-script
        self._check('not for us', '''
For database at %s:

Creating the following tables:
user
''' % (self.db_url, ),
                    kw=dict(argv=['create']))
        expected_metadata = MetaData()
        self.mytable.tometadata(expected_metadata)
        self._check_db(expected_metadata)
            
    def test_single_source(self):
        self._setup_config()
        # check         
        self._check('create','''
For database at %s:

Creating the following tables:
user
''' % (self.db_url, ))
        expected_metadata = MetaData()
        self.mytable.tometadata(expected_metadata)
        self._check_db(expected_metadata)

    def test_multi_source(self):
        
        # setup
        
        m1 = MetaData()
        t1 = Table('t1', m1,
                   Column('id', Integer, primary_key=True),
                   )
        m1 = MetaData()
        t2 = Table('t2', m1,
                   Column('jd', Integer, primary_key=True),
                   )
        self.config = Config(Source(t1),
                             Source(t2))
    
        # check 
        
        self._check('create','''
For database at %s:

Creating the following tables:
t1
t2
''' % (self.db_url, ))

        expected_metadata = MetaData()
        t1.tometadata(expected_metadata)
        t2.tometadata(expected_metadata)
        self._check_db(expected_metadata)
    
    def test_table_present(self):
        self._setup_config()
        self.mytable.create(create_engine(self.db_url))
        # check         
        self._check('create','''
For database at %s:

Refusing to create as the following tables exist:
user
''' % self.db_url)
        expected_metadata = MetaData()
        self.mytable.tometadata(expected_metadata)


class TestDrop(ScriptsMixin, ControlledTest):
    
    def _check_tables(self,*expected):
        compare(
            list(expected),
            Inspector.from_engine(self.engine).get_table_names()
            )
        
    def test_normal(self):
        self._check('drop','''
For database at %s:
Dropping all tables.
''' % self.db_url)

        self._check_tables()
    
    def test_failsafe(self):
        self.failsafe = False
        self._check('drop','''
For database at %s:
Refusing to drop all tables due to failsafe.
''' % self.db_url)

        self._check_tables('user')

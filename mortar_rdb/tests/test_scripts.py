# Copyright (c) 2011-2013 Simplistix Ltd, 2015 Chris Withers
# See license.txt for license details.
from argparse import ArgumentParser

from sqlalchemy import (
    Table, Column, Integer, MetaData, create_engine
)
from sqlalchemy.engine.reflection import Inspector
from testfixtures import (
    OutputCapture, compare, LogCapture)

from mortar_rdb.controlled import Scripts, Config, Source
from .base import ControlledTest, PackageTest

logger_name = 'mortar_rdb.controlled'

class ScriptsMixin:
    
    failsafe = True
    
    def _check(self, s, expected=None, kw=None):
        kw = kw or {}
        self.r.replace('sys.argv', ['X']+s.split())
        ex = None
        try:
            with OutputCapture() as output:
               self._callable()(**kw)
        except SystemExit as ex:
            if expected is SystemExit:
                return output.captured
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
        self.log = LogCapture()
        self.addCleanup(self.log.uninstall)

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
        self.db_url = 'junk://'
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

    def test_help(self):
        self._setup_config()
        output = self._check('--help', expected=SystemExit)
        self.assertTrue('acted on is at:\n'+self.db_url in output, output)
        self.assertTrue('in the current configuration:\nuser\n' in output, output)
        self.assertTrue('Create all the tables' in output, output)
        self.assertTrue('Drop all tables' in output, output)
        # db should be empty!
        self._check_db(MetaData())

    def test_password_in_help(self):
        self.db_url = "postgres://user:pw@localhost/db"
        self._setup_config()
        output = self._check('--help', expected=SystemExit)
        self.assertFalse('pw' in output, output)

    def test_help_with_our_parser(self):
        self._setup_config()
        parser = ArgumentParser()
        self.db_url = None
        obj = self._callable()
        obj.setup_parser(parser)
        with OutputCapture() as capture:
            try:
               parser.parse_args(['--help'])
            except SystemExit as ex:
                pass
        output = capture.captured
        self.assertFalse('acted on is at:\n' in output, output)
        self.assertTrue('in the current configuration:\nuser\n' in output, output)
        self.assertTrue('Create all the tables' in output, output)
        self.assertTrue('Drop all tables' in output, output)

    def test_create_without_using_call(self):
        self._setup_config()
        db_url = self.db_url
        self.db_url = None

        parser = ArgumentParser()
        obj = self._callable()
        obj.setup_parser(parser)

        args = parser.parse_args(['create'])
        obj.run(db_url, args)

        self.log.check(
            (logger_name, 'INFO', 'For database at '+db_url+':'),
            (logger_name, 'INFO', 'Creating the following tables:'),
            (logger_name, 'INFO', 'user'),
        )

        expected_metadata = MetaData()
        self.mytable.tometadata(expected_metadata)
        self.db_url = db_url
        self._check_db(expected_metadata)

    def test_create_without_using_call_url_option_preferred(self):
        self._setup_config()
        db_url = self.db_url
        self.db_url = None

        parser = ArgumentParser()
        obj = self._callable()
        obj.setup_parser(parser)

        args = parser.parse_args(['--url', db_url, 'create'])
        obj.run('absolute rubbish', args)

        self.log.check(
            (logger_name, 'INFO', 'For database at '+db_url+':'),
            (logger_name, 'INFO', 'Creating the following tables:'),
            (logger_name, 'INFO', 'user'),
        )

        expected_metadata = MetaData()
        self.mytable.tometadata(expected_metadata)
        self.db_url = db_url
        self._check_db(expected_metadata)

    def test_create_logging_not_printing(self):
        self._setup_config()
        parser = ArgumentParser()
        obj = self._callable()
        obj.setup_parser(parser)

        with OutputCapture() as output:
            args = parser.parse_args(['create'])
            obj.run('absolute rubbish', args)

        self.log.check(
            (logger_name, 'INFO', 'For database at '+self.db_url+':'),
            (logger_name, 'INFO', 'Creating the following tables:'),
            (logger_name, 'INFO', 'user'),
        )

        output.compare('')

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

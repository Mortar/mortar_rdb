# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb.controlled import create_repository
from mortar_rdb import create_engine
from mortar_rdb.controlled import (
    create_main, Scripts, Config, Source
    )
from migrate.versioning.schema import ControlledSchema
from sqlalchemy import Table, Column, Integer, String, Text, MetaData
from testfixtures import TempDirectory, Replacer, compare
from unittest import TestCase

import os,sys

class PackageTest(TestCase):

    def setUp(self):
        self.dir = TempDirectory(ignore=['pyc$','pyo$'])
        self.added_to_sys = []
        # now add the path to sys.path
        sys.path.append(self.dir.path)
        # keep a set of modules, so we can delete any that get added
        self.modules = set(sys.modules)
        # create a handy Replacer
        self.r = Replacer()
        # make a package
        self.dir.write('x/__init__.py','')
        self.dir.write('x/y/__init__.py','')
        self.dir.write('x/y/z/__init__.py','')
        
    def tearDown(self):
        self.r.restore()
        sys.path.remove(self.dir.path)
        for name in set(sys.modules)-self.modules:
            del sys.modules[name]
        self.dir.cleanup()
        
class RepoTest(PackageTest):

    def _make_repo(self,*location):
        location = location or ('x','y','z','repo')
        path = os.path.join(self.dir.path,*location)
        self.repo = create_repository(path,location[-1])
        compare(0,self.repo.latest)
        return self.repo
        
class ControlledTest(RepoTest):   

    def setUp(self):
        RepoTest.setUp(self)
        self._make_repo()
        self.metadata = MetaData()
        self.table = Table('user', self.metadata,
                        Column('id', Integer, primary_key=True),
                        )
        self.db_url = 'sqlite:///'+self.dir.getpath('sqlite.db')
        self.engine = create_engine(self.db_url)
        self.metadata.create_all(self.engine)
        self.config = Config(Source(self.repo.path,self.table))

    def _check_db(self,expected):
        self.assertEqual(
            expected,
            ControlledSchema(
                create_engine(self.db_url),
                self.repo.path
                ).version
            )
        

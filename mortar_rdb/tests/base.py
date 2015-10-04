# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb.controlled import Scripts, Config, Source
from sqlalchemy import (
    Table, Column, Integer, String, Text, MetaData, create_engine
    )
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
        self.dir.write('x/__init__.py', b'')
        self.dir.write('x/y/__init__.py', b'')
        self.dir.write('x/y/z/__init__.py', b'')
        
    def tearDown(self):
        self.r.restore()
        sys.path.remove(self.dir.path)
        for name in set(sys.modules)-self.modules:
            del sys.modules[name]
        self.dir.cleanup()
        
class ControlledTest(PackageTest):   

    def setUp(self):
        super(ControlledTest, self).setUp()
        self.metadata = MetaData()
        self.table = Table('user', self.metadata,
                        Column('id', Integer, primary_key=True),
                        )
        self.db_url = 'sqlite:///'+self.dir.getpath('sqlite.db')
        self.engine = create_engine(self.db_url)
        self.metadata.create_all(self.engine)
        self.config = Config(Source(self.table))

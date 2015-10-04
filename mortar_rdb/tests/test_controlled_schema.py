# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.

from unittest import TestCase

from sqlalchemy import Table, Column, Integer, String, MetaData
from testfixtures import (
    compare, TempDirectory, ShouldRaise,
    StringComparison as S
    )
from mortar_rdb.compat import PY2

from mortar_rdb.controlled import Source, scan, Config
from mortar_rdb.testing import TestingBase
from .base import PackageTest


class TestSource(TestCase):

    def setUp(self):
        self.dir = TempDirectory()
        self.tb = TestingBase()

    def tearDown(self):
        self.tb.restore()
        self.dir.cleanup()
    
    def test_table(self):
        metadata = MetaData()
        mytable = Table('user', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(40)),
                        )
        s = Source(mytable)

        # check we have the right tables
        compare(['user'], s.metadata.tables.keys())
        # check we have a new metadata object
        self.failIf(metadata is s.metadata)
        # check we have a copy of the table
        self.failIf(mytable is s.metadata.tables['user'])

    def test_class(self):

        class SomethingElse:
            pass

        if PY2:
            text = (
                "<class mortar_rdb.tests.test_controlled_schema."
                "SomethingElse at [0-9a-zA-Z]+>"
            )
        else:
            text = (
                "<class 'mortar_rdb.tests.test_controlled_schema."
                "TestSource.test_class.<locals>.SomethingElse'>"
            )

        with ShouldRaise(TypeError(S(
                    text+" must be a "
                    "Table object or a declaratively mapped model class."
                    ))):
            s = Source(SomethingElse)

class TestScan(PackageTest):

    def setUp(self):
        PackageTest.setUp(self)
        self.tb = TestingBase()
        
    def tearDown(self):
        self.tb.restore()
        PackageTest.tearDown(self)
        
    def test_doesnt_exist(self):
        if PY2:
            text = 'No module named package'
        else:
            text = "No module named 'test.package'"
        with ShouldRaise(ImportError(text)):
            scan('test.package.nothere')

    def test_module(self):
        # create module
        self.dir.write('somemodule.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class User(declarative_base()):
  __tablename__ = 'user'
  id = Column('id', Integer, primary_key=True)
""")
        
        s = scan('somemodule')

        self.failUnless(isinstance(s, Source))
        compare(['user'], s.metadata.tables.keys())
        
    def test_package(self):
        # create package
        package_dir = self.dir.makedir('somepackage')
        self.dir.write('somepackage/__init__.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class Table1(declarative_base()):
  __tablename__ = 'table1'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('somepackage/table2.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class Table2(declarative_base()):
  __tablename__ = 'table2'
  id = Column('id', Integer, primary_key=True)
""")

        s = scan('somepackage')

        self.failUnless(isinstance(s, Source))
        compare(['table1','table2'], sorted(s.metadata.tables.keys()))
        
    def test_package_import_loop(self):
        # this type of import loop occurs often
        package_dir = self.dir.makedir('demo')
        self.dir.write('demo/__init__.py', b'')
        self.dir.write('demo/model/__init__.py',
                       b"""
from mortar_rdb.controlled import Config,scan
config = Config(scan('demo'))
""")
        self.dir.write('demo/model/table.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer

class Table(declarative_base()):
  __tablename__ = 'table'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('demo/db.py', b"""
from demo.model import config
from mortar_rdb.controlled import Scripts

scripts = Scripts(
        'sqlite://',
        config,
        True,
        )

if __name__=='__main__':
    scripts()
""")
        self.dir.write('demo/run.py', b"from demo.model import config")
        
        # problem used to occur here

        from demo.model import config
        
        s = config.sources[0]
        self.failUnless(isinstance(s,Source))
        compare(['table'],sorted(s.metadata.tables.keys()))
        
    def test_type_of_things_to_scan_for(self):
        # create module
        self.dir.write('somemodule.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData

# a non-mapped old-style class
class Bad1: pass

# a non-mapped new-style class
class Bad2(object): pass

# table
metadata = MetaData()
table1 = Table('table1', metadata,
               Column('id', Integer, primary_key=True),
               )

# table that won't be found as not explicitly passed in
metadata = MetaData()
table2 = Table('table2', metadata,
               Column('id', Integer, primary_key=True),
               )

# declarative
class Model3(declarative_base()):
  __tablename__ = 'table3'
  id = Column('id', Integer, primary_key=True)

""")
        from somemodule import table1
        
        s = scan('somemodule',tables=[table1])

        self.failUnless(isinstance(s, Source))
        compare(['table1', 'table3'], sorted(s.metadata.tables.keys()))

    def test_single_table_inheritance(self):
        # create module
        self.dir.write('somemodule.py',
                       b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData

# the base
class BaseThing(declarative_base()):
  __tablename__ = 'table'
  id = Column('id', Integer, primary_key=True)

# type 1
class Type1Thing(BaseThing):
  foo = Column('foo', Integer)

# type 2
class Type2Thing(BaseThing):
  bar = Column('bar', Integer)

""")
        s = scan('somemodule')

        self.failUnless(isinstance(s,Source))
        compare(['table'], sorted(s.metadata.tables.keys()))
        # for fun:
        compare(['id', 'foo', 'bar'], s.metadata.tables['table'].c.keys())

    def test_ignore_imports_from_other_modules(self):
        self.dir.write('package0/__init__.py', b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class Model1(declarative_base()):
  __tablename__ = 'table1'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('package1/__init__.py', b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class Model2(declarative_base()):
  __tablename__ = 'table2'
  id = Column('id', Integer, primary_key=True)
from package0 import Model1
""")
        self.dir.write('package1/subpack/__init__.py', b"""
from mortar_rdb import declarative_base
from sqlalchemy import Table, Column, Integer
class Model3(declarative_base()):
  __tablename__ = 'table3'
  id = Column('id', Integer, primary_key=True)
""")
        
        s = scan('package1')

        self.failUnless(isinstance(s, Source))
        compare(['table2','table3'], sorted(s.metadata.tables.keys()))

    
class TestConfig(TestCase):

    def setUp(self):
        self.dir = TempDirectory()

    def tearDown(self):
        self.dir.cleanup()
        
    def test_table_in_multiple_sources(self):
        m1 = MetaData()
        t1 = Table('table', m1)
        m2 = MetaData()
        t2 = Table('table', m2)

        with ShouldRaise(
            ValueError("Tables present in more than one Source: table")
            ):
            Config(
                Source(t1),
                Source(t2),
                )

    def test_table_excludes(self):
        m1 = MetaData()
        t1 = Table('t1', m1)
        s1 = Source(t1)
        m2 = MetaData()
        t2 = Table('t2', m2)
        s2 = Source(t2)
        
        c = Config(s1, s2)

        compare({'t2'}, c.excludes[s1])
        compare({'t1'}, c.excludes[s2])

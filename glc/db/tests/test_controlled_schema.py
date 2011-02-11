from contextlib import nested
from glc.db import registerSession, declarative_base, drop_tables
from glc.db.controlled import (
    Source,scan,Config,create_repository,validate, ValidationException
    )
from glc.db.testing import TestingBase
from glc.testing.component import TestComponents
from migrate.exceptions import (
    InvalidRepositoryError,
    DatabaseNotControlledError
    )
from migrate.versioning.repository import Repository
from migrate.versioning.schema import ControlledSchema
from sqlalchemy import Table, Column, Integer, String, MetaData
from testfixtures import (
    compare, TempDirectory, ShouldRaise,
    StringComparison as S
    )
from unittest import TestCase

from .base import PackageTest, ControlledTest

import os

class TestCreateRepository(TestCase):

    def test_return_value(self):
        d = TempDirectory()
        r = create_repository(os.path.join(d.path,'test_repo'),'test')
        self.failUnless(isinstance(r,Repository))
        
    def test_no_manage(self):
        d = TempDirectory(ignore=['pyc$','pyo$'])
        create_repository(os.path.join(d.path,'test_repo'),'test')
        d.check_all('',
                    'test_repo/',
                    'test_repo/README',
                    'test_repo/__init__.py',
                    'test_repo/migrate.cfg',
                    'test_repo/versions/',
                    'test_repo/versions/__init__.py',
                    )
        compare("""[db_settings]
# Used to identify which repository this database is versioned under.
# You can use the name of your project.
repository_id=test

# The name of the database table used to track the schema version.
# This name shouldn't already be used by your project.
# If this is changed once a database is under version control, you'll need to 
# change the table name in each database too. 
version_table=migrate_version

# When committing a change script, Migrate will attempt to generate the 
# sql for all supported databases; normally, if one of them fails - probably
# because you don't have that database installed - it is ignored and the 
# commit continues, perhaps ending successfully. 
# Databases in this list MUST compile successfully during a commit, or the 
# entire commit will fail. List the databases your application will actually 
# be using to ensure your updates to that database work properly.
# This must be a list; example: ['postgres','sqlite']
required_dbs=[]
""",d.read('test_repo/migrate.cfg'))
                          
class TestCreateRepository(TestCase):

    def test_return_value(self):
        d = TempDirectory()
        r = create_repository(os.path.join(d.path,'test_repo'),'test')
        self.failUnless(isinstance(r,Repository))
        
class TestSource(TestCase):

    def setUp(self):
        self.dir = TempDirectory()
        # make a test repo
        repo = create_repository(
            os.path.join(self.dir.path,'test_repo'),
            'Test Repo'
            )
        self.repo_path = repo.path
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
        s = Source(self.repo_path,mytable)

        # check we have the right tables
        compare(['user'],s.metadata.tables.keys())
        # check we have a new metadata object
        self.failIf(metadata is s.metadata)
        # check we have a copy of the table
        self.failIf(mytable is s.metadata.tables['user'])
        # check repository
        self.failUnless(isinstance(s.repository,Repository))
        compare(self.repo_path,s.repository.path)

    def test_class(self):

        class SomethingElse:
            pass

        with ShouldRaise(TypeError(S(
                    "<class glc.db.tests.test_controlled_schema."
                    "SomethingElse at [0-9a-zA-Z]+> must be a "
                    "Table object or a declaratively mapped model class."
                    ))):
            s = Source(self.repo_path,SomethingElse)

    def test_invalid_repo(self):

        with ShouldRaise(InvalidRepositoryError):
            s = Source('',object())

class TestScan(PackageTest):

    def setUp(self):
        PackageTest.setUp(self)
        self.tb = TestingBase()
        
    def tearDown(self):
        self.tb.restore()
        PackageTest.tearDown(self)
        
    def test_doesnt_exist(self):
        with ShouldRaise(ImportError('No module named package.nothere')):
            scan('test.package.nothere')

    def test_module(self):
        # create a repo
        repo_path = os.path.join(self.dir.path,'db_versioning')
        create_repository(repo_path,'Test Repo')

        # create module
        self.dir.write('somemodule.py',
                       """
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class User(declarative_base()):
  __tablename__ = 'user'
  id = Column('id', Integer, primary_key=True)
""")
        
        s = scan('somemodule')

        self.failUnless(isinstance(s,Source))
        compare(['user'],s.metadata.tables.keys())
        compare(repo_path,s.repository.path)
        
    def test_package(self):
        # create package
        package_dir = self.dir.makedir('somepackage',path=True)
        self.dir.write('somepackage/__init__.py',
                       """
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Table1(declarative_base()):
  __tablename__ = 'table1'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('somepackage/table2.py',
                       """
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Table2(declarative_base()):
  __tablename__ = 'table2'
  id = Column('id', Integer, primary_key=True)
""")

        # create a repo
        repo_path = os.path.join(package_dir,'db_versioning')
        create_repository(repo_path,'Test Repo')
        
        s = scan('somepackage')

        self.failUnless(isinstance(s,Source))
        compare(['table1','table2'],sorted(s.metadata.tables.keys()))
        compare(repo_path,s.repository.path)
        
    def test_package_import_loop(self):
        # this type of import loop occurs often
        package_dir = self.dir.makedir('demo',path=True)
        self.dir.write('demo/__init__.py','')
        self.dir.write('demo/model/__init__.py',
                       """
from glc.db.controlled import Config,scan
config = Config(scan('demo'))
""")
        self.dir.write('demo/model/table.py',
                       """
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer

class Table(declarative_base()):
  __tablename__ = 'table'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('demo/db.py',"""
from demo.model import config
from glc.db.controlled import Scripts

scripts = Scripts(
        'sqlite://',
        config,
        True,
        )

if __name__=='__main__':
    scripts()
""")
        self.dir.write('demo/run.py',"from demo.model import config")
        
        # create a repo
        repo_path = os.path.join(package_dir,'db_versioning')
        create_repository(repo_path,'Test Repo')

        # problem used to occur here
        import demo.db

        from demo.model import config
        
        s = config.sources[0]
        self.failUnless(isinstance(s,Source))
        compare(['table'],sorted(s.metadata.tables.keys()))
        compare(repo_path,s.repository.path)
        
    def test_type_of_things_to_scan_for(self):
        # create a repo
        repo_path = os.path.join(self.dir.path,'db_versioning')
        create_repository(repo_path,'Test Repo')

        # create module
        self.dir.write('somemodule.py',
                       """
from glc.db import declarative_base
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

        self.failUnless(isinstance(s,Source))
        compare(['table1','table3'],sorted(s.metadata.tables.keys()))
        compare(repo_path,s.repository.path)

    def test_single_table_inheritance(self):
        # create a repo
        repo_path = os.path.join(self.dir.path,'db_versioning')
        create_repository(repo_path,'Test Repo')
        # create module
        self.dir.write('somemodule.py',
                       """
from glc.db import declarative_base
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
        compare(['table'],sorted(s.metadata.tables.keys()))
        # for fun:
        compare(['id', 'foo', 'bar'],s.metadata.tables['table'].c.keys())
        compare(repo_path,s.repository.path)

    def test_ignore_imports_from_other_modules(self):
        # create a repo
        repo_path = os.path.join(self.dir.path,'package1','db_versioning')
        create_repository(repo_path,'Test Repo')

        self.dir.write('package0/__init__.py',"""
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Model1(declarative_base()):
  __tablename__ = 'table1'
  id = Column('id', Integer, primary_key=True)
""")
        self.dir.write('package1/__init__.py',"""
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Model2(declarative_base()):
  __tablename__ = 'table2'
  id = Column('id', Integer, primary_key=True)
from package0 import Model1
""")
        self.dir.write('package1/subpack/__init__.py',"""
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Model3(declarative_base()):
  __tablename__ = 'table3'
  id = Column('id', Integer, primary_key=True)
""")
        
        s = scan('package1')

        self.failUnless(isinstance(s,Source))
        compare(['table2','table3'],sorted(s.metadata.tables.keys()))
        compare(repo_path,s.repository.path)

    
    def test_specify_repository_path(self):
        with TempDirectory() as d:
            
            repo_path = os.path.join(d.path,'repo')
            create_repository(repo_path,'Test Repo')
            
            self.dir.write('package/__init__.py',"""
from glc.db import declarative_base
from sqlalchemy import Table, Column, Integer
class Model(declarative_base()):
  __tablename__ = 'table'
  id = Column('id', Integer, primary_key=True)
""")
            
            s = scan('package',repository_path=repo_path)

            self.failUnless(isinstance(s,Source))
            compare(['table'],sorted(s.metadata.tables.keys()))
            compare(repo_path,s.repository.path)

class TestConfig(TestCase):

    def setUp(self):
        self.dir = TempDirectory()

    def tearDown(self):
        self.dir.cleanup()
        
    def test_table_in_multiple_sources(self):
        m1 = MetaData()
        t1 = Table('table', m1)
        r1 = create_repository(self.dir.getpath('r1'),'r1')
        m2 = MetaData()
        t2 = Table('table', m2)
        r2 = create_repository(self.dir.getpath('r2'),'r2')

        with ShouldRaise(
            ValueError("Tables present in more than one Source: table")
            ):
            Config(
                Source(r1.path,t1),
                Source(r2.path,t2),
                )

    def test_repo_in_multiple_sources(self):
        m1 = MetaData()
        t1 = Table('t1', m1)
        
        m2 = MetaData()
        t2 = Table('t2', m2)
        
        r = create_repository(self.dir.getpath('r'),'r')

        with ShouldRaise(
            ValueError("Repositories present in more than one Source: %s" % (
                    r.path
                    ))
            ):
            Config(
                Source(r.path,t1),
                Source(r.path,t2),
                )

    def test_table_excludes(self):
        m1 = MetaData()
        t1 = Table('t1', m1)
        r1 = create_repository(self.dir.getpath('r1'),'r1')
        s1 = Source(r1.path,t1)
        m2 = MetaData()
        t2 = Table('t2', m2)
        r2 = create_repository(self.dir.getpath('r2'),'r2')
        s2 = Source(r2.path,t2)
        
        c = Config(s1,s2)

        compare(set(['migrate_version','t2']),c.excludes[s1])
        compare(set(['migrate_version','t1']),c.excludes[s2])

    def test_tables_and_repos(self):
        m1 = MetaData()
        t1 = Table('t1', m1)
        r1 = create_repository(self.dir.getpath('r1'),'r1')
        s1 = Source(r1.path,t1)
        m2 = MetaData()
        t2 = Table('t2', m2)
        r2 = create_repository(self.dir.getpath('r2'),'r2')
        s2 = Source(r2.path,t2)
        
        c = Config(s1,s2)

        compare(set((r1.path,r2.path)),c.repos)
        compare(set(('t1','t2')),c.tables)

class TestValidationException(TestCase):

    def setUp(self):
        from glc.db.controlled import ValidationException
        self.e = ValidationException()

    def test_empty(self):
        # bool
        self.failIf(self.e)
        # repr
        compare(
            "<ValidationException>None</ValidationException>",
            repr(self.e)
            )
        # str
        compare(
            "<ValidationException>None</ValidationException>",
            str(self.e)
            )
        # body
        compare(None,self.e.body)
        # repos
        compare([],self.e.repos)
        # version mismatches
        compare({},self.e.version_mismatches)
        # diffs
        compare({},self.e.diffs)
        
    def test_version_mismatch(self):
        self.e.version_mismatch('foo',1,2)
        # bool
        self.failUnless(self.e)
        expected = """
<ValidationException>
Repository at:
foo
Version was 2, should be 1
</ValidationException>
""".strip()
        # repr
        compare(expected,repr(self.e))
        # str
        compare(expected,str(self.e))
        # body
        compare('''
Repository at:
foo
Version was 2, should be 1
''',self.e.body)
        # repos
        compare(['foo'],self.e.repos)
        # version mismatches
        compare({'foo':(2,1)},self.e.version_mismatches)
        # diffs
        compare({},self.e.diffs)

    def test_diff(self):
        self.e.diff('foo','xxx\nyyy\n')
        # bool
        self.failUnless(self.e)
        expected = """
<ValidationException>
Repository at:
foo
xxx
yyy
</ValidationException>
""".strip()
        # repr
        compare(expected,repr(self.e))
        # str
        compare(expected,str(self.e))
        # body
        compare('''
Repository at:
foo
xxx
yyy
''',self.e.body)
        # repos
        compare(['foo'],self.e.repos)
        # version mismatches
        compare({},self.e.version_mismatches)
        # diffs
        compare({'foo':'xxx\nyyy'},self.e.diffs)

    def test_both(self):
        self.e.version_mismatch('foo',1,2)
        self.e.diff('bar','xxx\nyyy\n')
        # bool
        self.failUnless(self.e)
        expected = """
<ValidationException>
Repository at:
bar
xxx
yyy

Repository at:
foo
Version was 2, should be 1
</ValidationException>
""".strip()
        # repr
        compare(expected,repr(self.e))
        # str
        compare(expected,str(self.e))
        # body
        compare('''
Repository at:
bar
xxx
yyy

Repository at:
foo
Version was 2, should be 1
''',self.e.body)
        # repos
        compare(['bar','foo'],self.e.repos)
        # version mismatches
        compare({'foo':(2,1)},self.e.version_mismatches)
        # diffs
        compare({'bar':'xxx\nyyy'},self.e.diffs)
    
class TestValidate(ControlledTest):

    def test_okay(self):
        # control DB, which should set it to the correct version
        ControlledSchema.create(self.engine,self.repo,self.repo.latest)
        compare(None,validate(self.engine,self.config))
    
    def test_empty(self):
        # add control
        ControlledSchema.create(self.engine,self.repo,self.repo.latest)
        # just the Source's table gone
        self.table.metadata.bind = self.engine
        self.table.drop()
        with ShouldRaise(ValidationException) as s:
            validate(self.engine,self.config)
        compare([self.repo.path],s.raised.repos)
        compare({},s.raised.version_mismatches)
        compare({self.repo.path:(
                    'Schema diffs:\n'
                    '  tables missing from database: user'
                    )},
                s.raised.diffs)
        # now everything!
        drop_tables(self.engine)
        with ShouldRaise(DatabaseNotControlledError) as s:
            validate(self.engine,self.config)
        
    def test_not_managed(self):
        with ShouldRaise(DatabaseNotControlledError) as s:
            validate(self.engine,self.config)
    
    def test_wrong_repository_version(self):
        # add control
        ControlledSchema.create(self.engine,self.repo,self.repo.latest)
        # now create a new script
        self.repo.create_script('Test')
        # and validate
        with ShouldRaise(ValidationException) as s:
            validate(self.engine,self.config)
        compare([self.repo.path],s.raised.repos)
        compare({self.repo.path:(0,1)},s.raised.version_mismatches)
        compare({},s.raised.diffs)

    def test_table_differences(self):
        # add control
        ControlledSchema.create(self.engine,self.repo,self.repo.latest)
        # screw with db to simulate problems
        self.metadata.bind = self.engine
        self.table.create_column(
            Column('foo', Integer)
            )
        # and validate
        with ShouldRaise(ValidationException) as s:
            validate(self.engine,self.config)
        compare([self.repo.path],s.raised.repos)
        compare({},s.raised.version_mismatches)
        compare({self.repo.path:
                 'Schema diffs:\n'
                 '  table with differences: user\n'
                 '    repository missing these columns: foo'},
                s.raised.diffs)

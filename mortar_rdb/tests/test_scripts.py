# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb import create_engine, drop_tables
from mortar_rdb.controlled import (
    create_main, Scripts, Config, Source
    )
from migrate.exceptions import (
    PathFoundError,InvalidRepositoryError,
    DatabaseAlreadyControlledError,
    DatabaseNotControlledError
    )
from migrate.versioning.repository import Repository
from migrate.versioning.schema import ControlledSchema
from migrate.versioning.schemadiff import getDiffOfModelAgainstDatabase
from mock import Mock
from sqlalchemy import Table, Column, Integer, String, Text, MetaData
from sqlalchemy.engine.reflection import Inspector
from testfixtures import (
    OutputCapture, Replacer, compare, ShouldRaise
    )
from unittest import TestCase

from .base import RepoTest, ControlledTest

import os

repo_table = Table(
    'migrate_version', MetaData(),
    Column('repository_id', String(250), primary_key=True),
    Column('repository_path', Text),
    Column('version', Integer),
    )

class ScriptsMixin:
    
    failsafe = True
    
    def _check(self,s,expected):
        self.r.replace('sys.argv',['X']+s.split())
        ex = None
        try:
            with OutputCapture() as output:
               self._callable()()
        except SystemExit,ex:
            # Catch this as the output will
            # likely tell us what the problem was
            pass
        output.compare(expected.lstrip())
        if ex is not None:
            raise ex
        
    def _callable(self):
        return Scripts(
            self.db_url,
            self.config,
            self.failsafe,
            )

class TestCreateMain(ScriptsMixin,RepoTest):

    def _callable(self):
        return create_main
    
    def _check_repo(self,s,expected,path,id):
        self._check(s,expected)
        compare(id,Repository(path).id)
        
    def test_repo_path(self):
        path = os.path.join(self.dir.path,'test_repo')
        self._check_repo(
            'repo --path %s --id test_id'%path,'''
Created repository with id 'test_id' at:
%s
'''% path,
            path,
            'test_id'
            )
        
    def test_repo_path_specified_name(self):
        # name is ignored!
        path = os.path.join(self.dir.path,'test_repo')
        full_path = os.path.join(path,'foo')
        self._check_repo(
            'repo --name foo --path %s --id test_id'%path,'''
Created repository with id 'test_id' at:
%s
'''% path,
            path,
            'test_id'
            )
        
    def test_repo_path_no_id(self):
        # b0rk!
        path = os.path.join(self.dir.path,'test_repo')
        with ShouldRaise(SystemExit(2)):
            self._check_repo(
                'repo --path %s '%path,'''
usage: X [-h] {repo,script} ...
X: error: --id must be specified when --path is used
''','','')
        
    def test_repo_package_defaults(self):
        path = os.path.join(self.dir.path,'x','y','z',
                            'db_versioning')
        self._check_repo(
            'repo --package x.y.z','''
Created repository with id 'x.y.z' at:
%s
'''% path,
            path,
            'x.y.z'
            )

    def test_repo_package_specified_name(self):
        path = os.path.join(self.dir.path,'x','y','z',
                            'foo')
        self._check_repo(
            'repo --name foo --package x.y.z','''
Created repository with id 'x.y.z' at:
%s
'''% path,
            path,
            'x.y.z'
            )

    def test_repo_package_specified_id(self):
        path = os.path.join(self.dir.path,'x','y','z',
                            'db_versioning')
        self._check_repo(
            'repo --id foo --package x.y.z','''
Created repository with id 'foo' at:
%s
'''% path,
            path,
            'foo'
            )

    def test_repo_invalid_package(self):
        with ShouldRaise(ImportError('No module named a.b.c')):
            self._check_repo('repo --package a.b.c','','','')

    def test_repo_already_there(self):
        self.test_repo_package_defaults()
        
        with ShouldRaise(PathFoundError(
                os.path.join(self.dir.path,'x','y','z',
                            'db_versioning')
                )):
            self._check_repo('repo --package x.y.z','','','')

    def test_script_path(self):
        self._make_repo()
        self._check(
                'script --path %s test_script'%self.repo.path,'''
Created script for version 1 at:
%s
''' % os.path.join(self.repo.path,'versions','001_test_script.py'))
        self.dir.check_dir('x/y/z/repo/versions',
                           '001_test_script.py',
                           '__init__.py',
                           )
        compare(1,self.repo.latest)

    def test_script_path_and_name(self):
        # name ignored
        self._make_repo()
        self._check(
                'script --path %s --name foo test_script'%self.repo.path,'''
Created script for version 1 at:
%s
''' % os.path.join(self.repo.path,'versions','001_test_script.py'))
        self.dir.check_dir('x/y/z/repo/versions',
                           '001_test_script.py',
                           '__init__.py',
                           )
        compare(1,self.repo.latest)

    def test_script_package(self):
        # fails as we have a non-defaul repo name
        self._make_repo()
        with ShouldRaise(InvalidRepositoryError(
                self.dir.getpath('x/y/z/db_versioning')
                )):
            self._check('script --package x.y.z test_script','')
        self.dir.check_dir('x/y/z/repo/versions',
                           '__init__.py',
                           )
        compare(0,self.repo.latest)
    
    def test_script_package_and_name(self):
        self._make_repo()
        self._check(
                'script --package x.y.z --name repo test_script','''
Created script for version 1 at:
%s
''' % os.path.join(self.repo.path,'versions','001_test_script.py'))
        self.dir.check_dir('x/y/z/repo/versions',
                           '001_test_script.py',
                           '__init__.py',
                           )
        compare(1,self.repo.latest)
    
    def test_script_invalid_package(self):
        with ShouldRaise(ImportError('No module named a.b.c')):
            self._check('script --package a.b.c test_script','')
    
    def test_multiple_scripts(self):
        self._make_repo()
        
        self._check(
                'script --package x.y.z --name repo foo_script','''
Created script for version 1 at:
%s
''' % os.path.join(self.repo.path,'versions','001_foo_script.py'))
        self.dir.check_dir('x/y/z/repo/versions',
                           '001_foo_script.py',
                           '__init__.py',
                           )
        compare(1,self.repo.latest)
        
        self._check(
                'script --package x.y.z --name repo bar_script','''
Created script for version 2 at:
%s
''' % os.path.join(self.repo.path,'versions','002_bar_script.py'))
        self.dir.check_dir('x/y/z/repo/versions',
                           '001_foo_script.py',
                           '002_bar_script.py',
                           '__init__.py',
                           )
        compare(2,self.repo.latest)


class TestCreate(ScriptsMixin,RepoTest):

    def setUp(self):
        RepoTest.setUp(self)
        self._make_repo()
        self.db_url = 'sqlite:///'+self.dir.getpath('sqlite.db')

    def _check_db(self,expected_metadata,expected_versions):
        engine = create_engine(self.db_url)
        diff = getDiffOfModelAgainstDatabase(
            expected_metadata,
            engine
            )
        self.failIf(diff,diff)
        for path,version in expected_versions.items():
            self.assertEqual(
                version,
                ControlledSchema(engine, path).version
                )
    def test_single_source(self):
        # setup
        metadata = MetaData()
        mytable = Table('user', metadata,
                        Column('id', Integer, primary_key=True),
                        )
        self.config = Config(Source(self.repo.path,mytable))

        # check 
        
        self._check('create','''
For database at %s:

Repository at:
%s
Creating the following tables:
user
Setting database version to:
0
''' % (self.db_url,self.repo.path))

        expected_metadata = MetaData()
        mytable.tometadata(expected_metadata)
        repo_table.tometadata(expected_metadata)
        self._check_db(expected_metadata,{
                self.repo.path:0,
                })

    def test_migrations_present(self):
        # setup
        self.repo.create_script('Test')
        metadata = MetaData()
        mytable = Table('user', metadata,
                        Column('id', Integer, primary_key=True),
                        )
        self.config = Config(Source(self.repo.path,mytable))

        # check 
        
        self._check('create','''
For database at %s:

Repository at:
%s
Creating the following tables:
user
Setting database version to:
1
''' % (self.db_url,self.repo.path))

        expected_metadata = MetaData()
        mytable.tometadata(expected_metadata)
        repo_table.tometadata(expected_metadata)
        self._check_db(expected_metadata,{
                self.repo.path:1,
                })
    
    def test_multi_source(self):
        
        # setup
        
        repo1 = self._make_repo('repo1')
        repo1.create_script('Test 1')
        repo1.create_script('Test 2')
        m1 = MetaData()
        t1 = Table('t1', m1,
                   Column('id', Integer, primary_key=True),
                   )
        repo2 = self._make_repo('repo2')
        repo2.create_script('Test 3')
        m1 = MetaData()
        t2 = Table('t2', m1,
                   Column('jd', Integer, primary_key=True),
                   )
        self.config = Config(Source(repo1.path,t1),
                             Source(repo2.path,t2))
    
        # check 
        
        self._check('create','''
For database at %s:

Repository at:
%s
Creating the following tables:
t1
Setting database version to:
2

Repository at:
%s
Creating the following tables:
t2
Setting database version to:
1
''' % (self.db_url,repo1.path,repo2.path))

        expected_metadata = MetaData()
        t1.tometadata(expected_metadata)
        t2.tometadata(expected_metadata)
        repo_table.tometadata(expected_metadata)
        self._check_db(expected_metadata,{
                repo1.path:2,
                repo2.path:1,
                })
    
    def test_table_present(self):
        # setup
        metadata = MetaData()
        mytable = Table('user', metadata,
                        Column('id', Integer, primary_key=True),
                        )
        mytable.create(create_engine(self.db_url))
        self.config = Config(Source(self.repo.path,mytable))

        # check 
        
        self._check('create','''
For database at %s:

Refusing to create as the following tables exist:
user
''' % self.db_url)

        expected_metadata = MetaData()
        mytable.tometadata(expected_metadata)


class TestDrop(ScriptsMixin,ControlledTest):
    
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

class TestValidate(ScriptsMixin,ControlledTest):

    def _check_validate(self,expected):
        with OutputCapture() as o:
            script = self._callable()
            result = script._validate()
        o.compare(expected.lstrip())
        return result
    
    def test_same(self):
        self.failUnless(self._check_validate(''))

    def test_different(self):
        m = MetaData(self.engine)
        self.table.tometadata(m)

        m.tables['user'].create_column(
            Column('jd', Integer),
            )
        self.failIf(self._check_validate('''
Repository at:
%s
Schema diffs:
  table with differences: user
    repository missing these columns: jd
''' % self.repo.path))

    def test_one_same_one_different(self):
        # setup
        drop_tables(self.engine)
        repo1 = self._make_repo('repo1')
        repo1.create_script('Test 1')
        repo1.create_script('Test 2')
        m1 = MetaData(self.engine)
        t1 = Table('t1', m1,
                   Column('id', Integer, primary_key=True),
                   )
        repo2 = self._make_repo('repo2')
        repo2.create_script('Test 3')
        m2 = MetaData(self.engine)
        t2 = Table('t2', m2,
                   Column('id', Integer, primary_key=True),
                   )
        self.config = Config(Source(repo1.path,t1),
                             Source(repo2.path,t2))
        m1.create_all()
        m2.create_all()
        t2.create_column(
            Column('jd', Integer),
            )

        # check
        self.failIf(self._check_validate('''
Repository at:
%s
Schema diffs:
  table with differences: t2
    repository missing these columns: jd
''' % self.repo.path))

class TestControl(ScriptsMixin,ControlledTest):

    def test_ok(self):
        
        self._check('control','''
For database at %s:

Repository at:
%s
Setting database version to:
0
'''%(self.db_url,self.repo.path))
        
        self._check_db(0)
        
    def test_not_matching(self):
        self.r.replace('mortar_rdb.controlled.Scripts._validate',
                       lambda a: False)
        
        self._check('control','''
For database at %s:

Cannot introduce controls as model does not match database.
'''%(self.db_url,))

        with ShouldRaise(
            DatabaseNotControlledError
            ):
            self._check_db(0)
        
    def test_already_versioned(self):
        self.test_ok()

        with ShouldRaise(
            DatabaseAlreadyControlledError
            ):
            self._check('control','')
        
class TestCheck(ScriptsMixin,ControlledTest):

    def test_not_versioned(self):
        with ShouldRaise(
            DatabaseNotControlledError
            ):
            self._check('check','')
            
    def test_okay(self):
        ControlledSchema.create(
            self.engine,
            self.config.sources[0].repository,
            0
            )
        self._check('check','''
For database at %s:

Repository at:
%s
Version is correctly at 0.
All tables are correct.
'''%(self.db_url,self.repo.path))
        
    def test_wrong_versions(self):
        self.repo.create_script('Test')
        ControlledSchema.create(
            self.engine,
            self.config.sources[0].repository,
            0
            )
        self._check('check','''
For database at %s:

Repository at:
%s
Version was 0, should be 1.
'''%(self.db_url,self.repo.path))
    
    def test_right_versions_but_differences(self):
        def _validate(self):
            print "_validate output"
        self.r.replace('mortar_rdb.controlled.Scripts._validate',
                       _validate)
        ControlledSchema.create(
            self.engine,
            self.config.sources[0].repository,
            0
            )
        self._check('check','''
For database at %s:

Repository at:
%s
Version is correctly at 0.
_validate output
'''%(self.db_url,self.repo.path))
        
    
class TestUpgrade(ScriptsMixin,ControlledTest):

    def setUp(self):
        ControlledTest.setUp(self)
        # create a script to take version past zero
        self.repo.create_script('Test')
        # Now control DB, which should set it to the correct version
        ControlledSchema.create(
            self.engine,
            self.config.sources[0].repository,
            self.config.sources[0].repository.latest
            )
        self._check_db(1)

    def test_no_steps_needed(self):
        # run upgrade
        self._check('upgrade','''
For database at %s:

Repository at:
%s
No upgrade necessary, version at 1.
'''%(self.db_url,self.repo.path))
        # check output
        pass

    def test_one_step_needed(self):
        self.repo.create_script('Test 2')
        # run upgrade
        self._check('upgrade','''
For database at %(db)s:

Repository at:
%(repo)s
1 -> 2 (%(repos)s002_Test_2.py)
done
'''%dict(
         db=self.db_url,
         repo=self.repo.path,
         repos=os.path.join(
                    self.repo.path,'versions',''
                    )))

    def test_two_steps_needed(self):
        self.repo.create_script('Test 1')
        self.repo.create_script('Test 2')
        # run upgrade
        self._check('upgrade','''
For database at %(db)s:

Repository at:
%(repo)s
1 -> 2 (%(repos)s002_Test_1.py)
done
2 -> 3 (%(repos)s003_Test_2.py)
done
'''%dict(
         db=self.db_url,
         repo=self.repo.path,
         repos=os.path.join(
                    self.repo.path,'versions',''
                    )))

    def test_model_doesnt_match_after_upgrade(self):
        # create a script
        self.repo.create_script('Test')
        # screw with db to simulate problems
        self.metadata.bind = self.engine
        self.table.create_column(
            Column('foo', Integer)
            )
        # run upgrade
        self._check('upgrade','''
For database at %(db)s:

Repository at:
%(repo)s
1 -> 2 (%(repos)s002_Test.py)
done

Repository at:
%(repo)s
Schema diffs:
  table with differences: user
    repository missing these columns: foo
'''%dict(
         db=self.db_url,
         repo=self.repo.path,
         repos=os.path.join(
                    self.repo.path,'versions',''
                    )))

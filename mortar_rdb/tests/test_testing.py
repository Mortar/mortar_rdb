# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.

import os

from mortar_rdb.testing import registerSession, TestingBase
from mortar_rdb import getSession, declarative_base
from mortar_rdb.controlled import Config, Source
from testfixtures.components import TestComponents
from mock import Mock
from sqlalchemy.pool import StaticPool
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base as sa_declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.types import Integer, String
from testfixtures import Replacer, compare, TempDirectory, OutputCapture
from unittest import TestCase

class TestRegisterSessionFunctional(TestCase):

    def setUp(self):
        self.dir = TempDirectory()
        self.components = TestComponents()

    def tearDown(self):
        self.components.uninstall()
        self.dir.cleanup()
        
    def test_functional(self):
        Base = sa_declarative_base()
        class Model(Base):
            __tablename__ = 'model'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            
        registerSession(
            transactional=False,
            config=Config(Source(Model.__table__)))
        session = getSession()
        session.add(Model(name='foo'))
        session.commit()

    def test_functional_metadata(self):
        Base = sa_declarative_base()
        class Model(Base):
            __tablename__ = 'model'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            
        registerSession(
            transactional=False,
            metadata=Base.metadata
            )
        session = getSession()
        session.add(Model(name='foo'))
        session.commit()

    def test_functional_echo_but_no_url(self):
        with Replacer() as r:
            # make sure there's no DBURL
            r.replace('os.environ',dict())
            # hoover up the logging ;-)
            with OutputCapture():
                registerSession(echo=True)

    def test_tricky_to_delete(self):
        # respect any DB_URL set here so that
        # we sure the real db here to make sure
        # delete works across all our DB types...
        db_path = os.environ.get(
            'DB_URL',
            'sqlite:///'+os.path.join(self.dir.path,'test.db')
            )

        Base = sa_declarative_base()

        class Model1(Base):
            __tablename__ = 'model1'
            id = Column(Integer, primary_key=True)
            model2_id = Column(Integer,ForeignKey('model2.id'))
            model2 = relationship("Model2")

        class Model2(Base):
            __tablename__ = 'model2'
            id = Column('id', Integer, primary_key=True)

        config = Config(Source(
                Model1.__table__,
                Model2.__table__
                ))
        
        # create in one session
        registerSession(db_path,name='create',
                        transactional=False,
                        config=config)
        m1 = Model1()
        m2 = Model2()
        m1.model2 = m2
        session = getSession('create')
        session.add(m1)
        session.add(m2)
        session.commit()
        compare(session.query(Model1).count(),1)
        compare(session.query(Model2).count(),1)

        # now register another session which should
        # blow the above away
        registerSession(db_path,name='read',
                        transactional=False,
                        config=config)
        session = getSession('read')
        compare(session.query(Model1).count(),0)
        compare(session.query(Model2).count(),0)

    def test_only_some_packages(self):
        Base = sa_declarative_base()
        
        class Model1(Base):
            __tablename__ = 'model1'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            
        class Model2(Base):
            __tablename__ = 'model2'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            
        registerSession(
            transactional=False,
            config=Config(Source(Model1.__table__)))

        # only table1 should have been created!
        compare(
            [u'model1'],
            Inspector.from_engine(getSession().bind).get_table_names()
            )
            
class TestRegisterSessionCalls(TestCase):

    def setUp(self):
        self.components = TestComponents()
        self.r = Replacer()
        self.m = Mock()
        self.r.replace('mortar_rdb.testing.realRegisterSession',
                       self.m.realRegisterSession)
        self.r.replace('mortar_rdb.testing.create_engine',
                       self.m.create_engine)
        # mock out for certainty
        # self.r.replace('mortar_rdb.testing.???',Mock())
        # mock out for table destruction
        getSession = Mock()
        bind = getSession.return_value.bind
        bind.dialect.inspector.return_value = inspector = Mock()
        inspector.get_table_names.return_value = ()
        self.r.replace('mortar_rdb.testing.getSession',getSession)

    def tearDown(self):
        self.r.restore()
        self.components.uninstall()
        
    def test_default_params(self):
        # ie: no DB_URL!
        self.r.replace('os.environ',dict())
        registerSession()
        compare([
            ('create_engine',
             ('sqlite://',),
             {'poolclass': StaticPool,
              'echo': False}),
            ('realRegisterSession',
             (None, u'', self.m.create_engine.return_value, False, True, True, None, None), {}),
            ],self.m.method_calls)

    def test_specified_params(self):
        registerSession(
            url='x://',
            name='foo',
            echo=True,
            transactional=False,
            scoped=False,
            )
        compare([
                ('realRegisterSession',
                 ('x://', u'foo', None, True, False, False, None, None), {}),
                ],self.m.method_calls)

    def test_echo_but_no_url(self):
        # make sure there's no DBURL
        self.r.replace('os.environ',dict())
        registerSession(echo=True)
        compare([
            ('create_engine',
             ('sqlite://',),
             {'poolclass': StaticPool,
              'echo': True}),
            ('realRegisterSession',
             (None, u'', self.m.create_engine.return_value, False, True, True, None, None), {}),
            ],self.m.method_calls)
        

    def test_engine_passed(self):
        engine = object()
        registerSession(
            engine=engine,
            )
        compare([
                ('realRegisterSession',
                 (None, u'', engine, False, True, True, None, None), {}),
                ],self.m.method_calls)

    def test_url_from_environment(self):
        self.r.replace('os.environ',dict(
                DB_URL = 'x://'
                ))
        registerSession()
        compare([
                ('realRegisterSession',
                 ('x://', u'', None, False, True, True, None, None), {}),
                ],self.m.method_calls)

    def test_engine_overrides_environment(self):
        self.r.replace('os.environ',dict(
                DB_URL = 'x://'
                ))
        engine = object()
        registerSession(engine=engine)
        compare([
                ('realRegisterSession',
                 (None, u'', engine, False, True, True, None, None), {}),
                ],self.m.method_calls)

    def test_extension(self):
        engine = object()
        extension = object()
        registerSession(engine=engine,extension=extension)
        compare([
                ('realRegisterSession',
                 (None, u'', engine, False, True, True, None, extension), {}),
                ],self.m.method_calls)

class TestTestingBase(TestCase):

    def test_manual(self):
        b1 = declarative_base()
        tb = TestingBase()
        b2 = declarative_base()
        tb.restore()
        b3 = declarative_base()
        # checks
        self.failIf(b1 is b2)
        self.failIf(b3 is b2)
        self.failUnless(b1 is b3)

    def test_context_manager(self):
        b1 = declarative_base()
        with TestingBase():
            b2 = declarative_base()
        b3 = declarative_base()
        # checks
        self.failIf(b1 is b2)
        self.failIf(b3 is b2)
        self.failUnless(b1 is b3)

from glc.db import registerSession
from glc.db.interfaces import ISession
from mock import Mock
from testfixtures import (
    Replacer, Comparison as C, compare, ShouldRaise
    )
from unittest import TestCase
from zope.sqlalchemy import ZopeTransactionExtension

class TestUtility(TestCase):

    def setUp(self):
        self.r = Replacer()
        self.m = Mock()
        self.r.replace('glc.db.create_engine',self.m.create_engine)
        self.engine = self.m.create_engine.return_value
        self.r.replace('glc.db.validate',self.m.validate)
        self.r.replace('glc.db.scoped_session',self.m.scoped_session)
        self.ScopedSession = self.m.scoped_session.return_value
        self.r.replace('glc.db.sessionmaker',self.m.sessionmaker)
        self.Session = self.m.sessionmaker.return_value
        self.r.replace('glc.db.getSiteManager',self.m.getSiteManager)
        self.m.getSiteManager.return_value = self.m.registry

    def tearDown(self):
        self.r.restore()
        
    def test_mysql(self):
        self.engine.dialect.name='mysql'
        registerSession(url='mysql://foo')
        compare([
                ('create_engine', ('mysql://foo',), {'echo':False}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension),
                  'twophase': True,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_postgres(self):
        self.engine.dialect.name='postgresql'
        registerSession(url='postgres://foo')
        compare([
                ('create_engine', ('postgres://foo',), {'echo':False}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension),
                  'twophase': True,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_sqlite(self):
        registerSession(url='sqlite://foo')
        compare([
                ('create_engine', ('sqlite://foo',), {'echo':False}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension)
                  },),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_not_threaded(self):
        registerSession(url='mysql://foo',
                        threaded=False,transaction=False)
        compare([
                ('create_engine', ('mysql://foo',), {'echo':False}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.Session,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_threaded_no_transactions(self):
        registerSession(engine=self.m.engine2,
                        threaded=True,transaction=False)
        compare([
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.m.engine2}),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_engine(self):
        registerSession(engine=self.m.engine2)
        compare([
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.m.engine2,
                  'extension': C(ZopeTransactionExtension)}),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_url(self):
        registerSession('mysql://foo')
        compare([
                ('create_engine', ('mysql://foo',), {'echo':False}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension)}),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_echo(self):
        registerSession(url='mysql://foo',echo=True)
        compare([
                ('create_engine', ('mysql://foo',), {'echo':True}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension)}),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_echo_and_engine(self):
        with ShouldRaise(
            TypeError('Cannot specify echo if an engine is passed')
            ):
            registerSession(engine=self.m.engine2,echo=True)
        
        compare([],self.m.method_calls)

    def test_engine_and_url(self):
        with ShouldRaise(
            TypeError('Must specify engine or url, but not both')
            ):
            registerSession(url='mysql://',engine=self.m.engine2)
        
        compare([],self.m.method_calls)

    def test_neither_engine_nor_url(self):
        with ShouldRaise(
            TypeError('Must specify engine or url, but not both')
            ):
            registerSession()
        
        compare([],self.m.method_calls)

    def test_transactional_but_not_threaded(self):
        with ShouldRaise(
            TypeError('Transactions can only be managed in multi-threaded code')
            ):
            registerSession(url='mysql://',
                            transaction=True,threaded=False)
        
        compare([],self.m.method_calls)

    def test_controlled(self):
        config = object()
        registerSession(url='sqlite://foo',config=config)
        compare([
                ('create_engine', ('sqlite://foo',), {'echo':False}),
                ('validate',(self.engine,config),{}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'extension': C(ZopeTransactionExtension),
                  },),
                ('scoped_session', (self.Session,), {}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_controlled_raises(self):
        def raiser(*args):
            # likely to be ValidationException or a migrate complaint
            raise Exception()
        self.m.validate.side_effect = raiser
        config = object()
        with ShouldRaise():
            registerSession(url='sqlite://foo',config=config)
        compare([
                ('create_engine', ('sqlite://foo',), {'echo':False}),
                ('validate',(self.engine,config),{}),
                ],self.m.method_calls)

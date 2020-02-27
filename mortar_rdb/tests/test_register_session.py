from unittest import TestCase

from mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from testfixtures import (
    Replacer, compare, ShouldRaise, LogCapture
)
from zope.sqlalchemy.datamanager import STATUS_CHANGED

from mortar_rdb import register_session
from mortar_rdb.interfaces import ISession


class TestUtility(TestCase):

    def setUp(self):
        self.r = Replacer()
        self.m = Mock()
        self.r.replace('mortar_rdb.create_engine',self.m.create_engine)
        self.engine = self.m.create_engine.return_value
        self.engine.url.password = None
        self.r.replace('mortar_rdb.scoped_session',self.m.scoped_session)
        self.ScopedSession = self.m.scoped_session.return_value
        self.r.replace('mortar_rdb.sessionmaker',self.m.sessionmaker)
        self.Session = self.m.sessionmaker.return_value
        self.r.replace('mortar_rdb.getSiteManager',self.m.getSiteManager)
        self.m.getSiteManager.return_value = self.m.registry
        self.r.replace('mortar_rdb.register',self.m.register)

    def tearDown(self):
        self.r.restore()
        
    def test_mysql(self):
        self.engine.dialect.name='mysql'
        register_session(url='mysql://foo')
        compare([
                ('create_engine', ('mysql://foo',), {'echo':None}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'twophase': True,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_postgres(self):
        self.engine.dialect.name='postgresql'
        register_session(url='postgres://foo')
        compare([
                ('create_engine', ('postgres://foo',), {'echo':None}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  'twophase': True,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_no_twophase(self):
        self.engine.dialect.name='postgresql'
        register_session(url='postgres://foo',twophase=False)
        compare([
                ('create_engine', ('postgres://foo',), {'echo':None}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_sqlite(self):
        register_session(url='sqlite://foo')
        compare([
                ('create_engine', ('sqlite://foo',), {'echo':None}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine,
                  },),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_not_scoped(self):
        register_session(url='mysql://foo',
                        scoped=False,transactional=False)
        compare([
                ('create_engine', ('mysql://foo',), {'echo':None}),
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

    def test_scoped_no_transactions(self):
        self.m.engine2.url.password = 'pass'
        register_session(engine=self.m.engine2,
                        scoped=True,transactional=False)
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
        self.m.engine2.url.password = 'pass'
        register_session(engine=self.m.engine2)
        compare([
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.m.engine2}),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_url(self):
        register_session('mysql://foo')
        compare([
                ('create_engine', ('mysql://foo',), {'echo':None}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine}),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
                ('getSiteManager', (), {}),
                ('registry.registerUtility',
                 (self.ScopedSession,),
                 {'name': u'',
                  'provided': ISession})
                ],self.m.method_calls)

    def test_echo(self):
        register_session(url='mysql://foo',echo=True)
        compare([
                ('create_engine', ('mysql://foo',), {'echo':True}),
                ('sessionmaker',
                 (),
                 {'autocommit': False,
                  'autoflush': True,
                  'bind': self.engine}),
                ('scoped_session', (self.Session,), {}),
                ('register', (self.ScopedSession,), {'initial_state': STATUS_CHANGED}),
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
            register_session(engine=self.m.engine2,echo=True)
        
        compare([],self.m.method_calls)

    def test_engine_and_url(self):
        with ShouldRaise(
            TypeError('Must specify engine or url, but not both')
            ):
            register_session(url='mysql://',engine=self.m.engine2)
        
        compare([],self.m.method_calls)

    def test_neither_engine_nor_url(self):
        with ShouldRaise(
            TypeError('Must specify engine or url, but not both')
            ):
            register_session()
        
        compare([],self.m.method_calls)

    def test_transactional_but_not_scoped(self):
        with ShouldRaise(
            TypeError('Transactions can only be managed when using scoped sessions')
            ):
            register_session(url='mysql://',
                            transactional=True,scoped=False)
        
        compare([],self.m.method_calls)

    def test_logging_normal(self):
        self.engine.url = make_url('sqlite://')
        
        with LogCapture() as l:
            register_session('sqlite://')
            
        l.check((
                'mortar_rdb',
                'INFO',
                "Registering session for sqlite:// with name ''"
                ))

    def test_logging_password(self):
        self.engine.url = make_url('mysql://user:pass@localhost/db')
        
        with LogCapture() as l:
            register_session('sqlite://')
            
        l.check((
                'mortar_rdb',
                'INFO',
                "Registering session for "
                "mysql://user:***@localhost/db with name ''"
                ))

    def test_logging_username_password_db_same(self):
        self.engine.url=make_url('mysql://proj:proj@localhost/proj')

        with LogCapture() as l:
            register_session('sqlite://')

        l.check((
                'mortar_rdb',
                'INFO',
                "Registering session for "
                "mysql://proj:***@localhost/proj with name ''"
                ))

    def test_logging_name(self):
        engine = create_engine('sqlite://')
        self.engine.url=engine.url

        with LogCapture() as l:
            register_session('sqlite://','foo')
            
        l.check((
                'mortar_rdb',
                'INFO',
                "Registering session for sqlite:// with name 'foo'"
                ))

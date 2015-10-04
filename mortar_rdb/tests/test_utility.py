# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb import register_session, get_session
from mortar_rdb.compat import empty_str
from mortar_rdb.interfaces import ISession
from testfixtures.components import TestComponents
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String
from threading import Thread
from testfixtures import (
    ShouldRaise,compare,generator,Comparison as C, LogCapture
    )
from unittest import TestCase
from zope.component import getSiteManager
from zope.component.interfaces import ComponentLookupError

import transaction

class TestUtility(TestCase):

    def setUp(self):
        self.components = TestComponents()
        self.Base = declarative_base()
        class Model(self.Base):
            __tablename__ = 'model'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
        self.Model = Model

    def tearDown(self):
        self.components.uninstall()

    def test_how_to_create(self):
        register_session('sqlite://', transactional=False)
        # at this stage we have no tables
        session = get_session()
        session.add(self.Model(name='foo'))
        # so we get an error
        with ShouldRaise(OperationalError):
            session.commit()
        # ...which we then need to abort:
        session.rollback()
        # if we know we have no tables, we can do:
        self.Base.metadata.create_all(session.bind)
        # now we can commit:
        session.add(self.Model(name='foo'))
        session.commit()
        # ...and get stuff back:
        self.assertEqual(1,session.query(self.Model).count())
        
    def test_get_in_view(self):
        register_session('sqlite://')
        register_session('sqlite://','foo')

        # create the tables
        session1 = get_session()
        session2 = get_session('foo')
        with transaction.manager:
            self.Base.metadata.create_all(session1.bind)
            self.Base.metadata.create_all(session2.bind)
        
        # this is what you'd do in views:
        session = get_session()
        session.add(self.Model(id=1,name='foo'))
        model1 = session.query(self.Model).one()
        self.assertEqual(model1.id,1)
        self.assertEqual(model1.name,'foo')

        # or with a name...
        session = get_session('foo')
        session.add(self.Model(id=1,name='foo'))
        model2 = session.query(self.Model).one()
        self.assertEqual(model2.id,1)
        self.assertEqual(model2.name,'foo')

        # paranoia
        self.failIf(model1 is model2)
        
    def test_register(self):
        register_session('sqlite://')

        # create the tables
        session = get_session()
        self.Base.metadata.create_all(session.bind)
        
        # check registrations
        compare(generator(
                C('zope.component.registry.UtilityRegistration',
                  component=C('sqlalchemy.orm.scoping.ScopedSession'),
                  factory=None,
                  info=u'',
                  name=u'',
                  provided=ISession,
                  registry=self.components.registry
                  )),self.components.registry.registeredUtilities())
        
        # this is what get_session goes:
        session = getSiteManager().getUtility(ISession)
        
        session.add(self.Model(id=1,name='foo'))
        model = session.query(self.Model).one()
        self.assertEqual(model.id,1)
        self.assertEqual(model.name,'foo')

    def test_register_with_name(self):
        register_session('sqlite://','foo')

        # check registrations
        compare(generator(
                C('zope.component.registry.UtilityRegistration',
                  component=C('sqlalchemy.orm.scoping.ScopedSession'),
                  factory=None,
                  info=u'',
                  name=u'foo',
                  provided=ISession,
                  registry=self.components.registry
                  )),self.components.registry.registeredUtilities())
        
        registry = getSiteManager()
        
        # check we don't register with no name:
        with ShouldRaise(ComponentLookupError(ISession, u'')):
            registry.getUtility(ISession)

        # check we do with the right name
        self.failUnless(isinstance(
                registry.getUtility(ISession,'foo')(),
                Session
                ))


    def test_transaction(self):
        register_session('sqlite://')
        
        # functional
        with transaction.manager:
            session = get_session()
            self.Base.metadata.create_all(session.bind)
            session.add(self.Model(id=1,name='foo'))
        
        compare(1,
                session.scalar(self.Model.__table__.select().count()))
        
    def test_transaction_no_session_usage(self):
        register_session('sqlite://')

        # functional
        with transaction.manager:
            session = get_session()
            self.Base.metadata.create_all(session.bind)
            session.execute(
                self.Model.__table__.insert().values(name='test')
                )

        compare(1,
                session.scalar(self.Model.__table__.select().count()))
            
        
    def test_no_transaction(self):
        register_session('sqlite://',transactional=False)
        
        # functional
        session = get_session()
        self.Base.metadata.create_all(session.bind)
        session.add(self.Model(id=1,name='foo'))
        session.commit()
    
        compare(1,
                session.scalar(self.Model.__table__.select().count()))

    def test_different_sessions_per_thread(self):
        
        register_session('sqlite://')

        class TestThread(Thread):
            def run(self):
                self.resulting_session = get_session()

        t1 = TestThread()
        t1.start()
        t2 = TestThread()
        t2.start()
        t1.join()
        t2.join()

        self.assertNotEquals(
            id(t1.resulting_session),
            id(t2.resulting_session),
            )

    def test_different_sessions_when_async(self):
        
        register_session('sqlite://',
                        scoped=False, transactional=False)

        s1 = get_session()
        s2 = get_session()

        self.assertNotEquals(id(s1),id(s2))
        
    def test_logging_functional(self):
        
        with LogCapture() as l:
            register_session('sqlite://')
            
        l.check((
                'mortar_rdb',
                'INFO',
                "Registering session for 'sqlite://' with name "+empty_str
                ))

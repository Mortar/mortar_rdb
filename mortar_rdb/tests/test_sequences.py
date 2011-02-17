from mortar_rdb import drop_tables, getSession, registerSession
from mortar_rdb.controlled import Config
from mortar_rdb.sequence import registerSequence, getSequence
from mortar_rdb.sequence.generic import source,sequences
from mortar_rdb.testing import registerSession as test_registerSession
from testfixtures.components import TestComponents
from sqlalchemy.exc import ProgrammingError,OperationalError
from testfixtures import TempDirectory,compare,ShouldRaise
from unittest import TestCase
from zope.component.interfaces import ComponentLookupError

import os, transaction

class TestGeneric(TestCase):

    # we don't pass an impl here, which uses the
    # generic implementation that we want to test

    def setUp(self):
        self.dir = TempDirectory()
        self.components = TestComponents()
        # make sure the db is on disk
        self.url = os.environ.get(
            'DB_URL',
            'sqlite:///'+self.dir.getpath('test.db')
            )
        # register two sessions to check what happens
        # when different threads/processes fiddle with
        # the same sequence, the first one is set ut
        # with testing's registerSession so we get the tables
        # set up.
        test_registerSession(self.url,'1',
                             config=Config(source),
                             transactional=False)
        # the second one is a normal session, so we don't
        # splat the tables created above.
        # (we need to do it this way 'cos of some sqlite flakeyness
        #  when tables are created in a session opened after the session
        #  that then tries to use them)
        registerSession(self.url,'2',
                        config=Config(source),
                        transactional=False)

    def tearDown(self):
        self.dir.cleanup()
        self.components.uninstall()

    def test_no_table(self):
        drop_tables(getSession('1').bind)
        try:
            registerSequence('test',getSession('1'))
        except (ProgrammingError,OperationalError):
            pass
        else:
            self.fail('huh?')

    def test_no_row_in_table(self):
        registerSequence('test',getSession('1'))
        session = getSession('1')
        session.execute('delete from sequences')
        seq = getSequence('test')
        with ShouldRaise(
            ValueError("No result returned for sequence 'test'",)
            ):
            seq.next(session)
    
    def test_no_sequence(self):
        with ShouldRaise(ComponentLookupError):
            seq = getSequence('test')
    
    def test_register_not_existing(self):
        session = getSession('1')
        registerSequence('test',session)
        
        seq = getSequence('test')

        compare(1,seq.next(session))
        compare(2,seq.next(session))
        compare(3,seq.next(session))

    def test_register_existing(self):
        session = getSession('1')
        registerSequence('test',session)
        registerSequence('test',session)
        
        seq = getSequence('test')

        compare(1,seq.next(session))
        compare(2,seq.next(session))
        compare(3,seq.next(session))

    def test_get_multiple_connections(self):
        
        session1 = getSession('1')
        session2 = getSession('2')
        
        registerSequence('test',session1)
        
        seq = getSequence('test')

        # the commits are needed here as otherwise
        # the .next() call will block.
        compare(1,seq.next(session1))
        session1.commit()
        compare(2,seq.next(session2))
        session2.commit()
        compare(3,seq.next(session1))

    def test_exception_during_register(self):
        session = getSession('1')
        registerSequence('test',getSession('1'))
        compare(1,session.scalar(sequences.count()))

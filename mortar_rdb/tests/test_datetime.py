# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from datetime import datetime
from mortar_rdb import get_session
from mortar_rdb.testing import register_session
from testfixtures.components import TestComponents
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column
from sqlalchemy.types import  Integer, DateTime
from unittest import TestCase

import transaction

class TestDateTime(TestCase):
    # illustrative tests for the suck that is MySQL DateTime

    def setUp(self):
        self.components = TestComponents()
        
    def tearDown(self):
        self.components.uninstall()

    def test_subsecond_accuracy(self):

        Base = declarative_base()

        class Sucktastic(Base):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)
            value = Column(DateTime)
            
        register_session(metadata=Base.metadata)

        with transaction.manager:
            session = get_session()
            session.add(Sucktastic(value=datetime(2001,1,1,10,1,2,3)))

        session = get_session()
        
        suck = session.query(Sucktastic).one()

        if session.bind.name=='mysql':
            # whoops, there goes our sub-second info
            self.assertEqual(suck.value,datetime(2001,1,1,10,1,2))
        else:
            self.assertEqual(suck.value,datetime(2001,1,1,10,1,2,3))

        session.rollback()

# Copyright (c) 2011 Simplistix Ltd
# See license.txt for license details.

from mortar_rdb import declarative_base
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import DeclarativeMeta
from testfixtures import Replacer, ShouldRaise
from unittest import TestCase
from mortar_rdb.compat import PY2


class TestGetBase(TestCase):

    def setUp(self):
        self.r = Replacer()

    def tearDown(self):
        self.r.restore()
        
    def test_first_call(self):
        self.r.replace('mortar_rdb._bases',{})
        b1 = declarative_base()
        b2 = declarative_base()
        self.assertTrue(isinstance(b1,DeclarativeMeta))
        self.assertTrue(b1 is b2)

    def test_existing_base(self):
        base = object()
        self.r.replace('mortar_rdb._bases',{():base})
        self.assertTrue(base is declarative_base())

    def test_arguments(self):
        # the first parameter to declarative_base :-)
        engine = object()
        if PY2:
            text = 'declarative_base() takes exactly 0 arguments (1 given)'
        else:
            text = 'declarative_base() takes 0 positional arguments but 1 was given'

        with ShouldRaise(TypeError(text)):
            declarative_base(engine)

    def test_parameters_engine(self):
        engine = create_engine('sqlite://')

        b1 = declarative_base(bind=engine)
        b2 = declarative_base(bind=engine)

        self.assertTrue(b1 is b2)
        self.assertTrue(b1.metadata.bind is engine)
        self.assertTrue(b2.metadata.bind is engine)
    
    def test_parameters_metadata(self):
        metadata = MetaData()

        b1 = declarative_base(metadata=metadata)
        b2 = declarative_base(metadata=metadata)

        self.assertTrue(b1 is b2)
        self.assertTrue(b1.metadata is metadata)
        self.assertTrue(b2.metadata is metadata)
    
    def test_parameters_metaclass(self):
        class MyMeta(DeclarativeMeta):
            pass

        b1 = declarative_base(metaclass=MyMeta)
        b2 = declarative_base(metaclass=MyMeta)

        self.assertTrue(b1 is b2)
        self.assertTrue(isinstance(b1,MyMeta))
        self.assertTrue(isinstance(b2,MyMeta))

    def test_different_bases(self):
        engine = create_engine('sqlite://')
        metadata = MetaData()
        class MyMeta(DeclarativeMeta):
            pass

        b1 = declarative_base(bind=engine)
        b2 = declarative_base(metadata=metadata)
        b3 = declarative_base(metaclass=MyMeta)

        self.assertFalse(b1 is b2)
        self.assertFalse(b1 is b3)
        self.assertFalse(b2 is b3)

        self.assertTrue(b1.metadata.bind is engine)
        self.assertFalse(b2.metadata.bind is engine)
        self.assertFalse(b3.metadata.bind is engine)
        
        self.assertFalse(b1.metadata is metadata)
        self.assertTrue(b2.metadata is metadata)
        self.assertFalse(b3.metadata is metadata)

        self.assertFalse(isinstance(b1,MyMeta))
        self.assertFalse(isinstance(b2,MyMeta))
        self.assertTrue(isinstance(b3,MyMeta))

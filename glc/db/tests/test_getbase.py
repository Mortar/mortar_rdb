from glc.db import getBase
from sqlalchemy.ext.declarative import DeclarativeMeta
from testfixtures import Replacer
from unittest import TestCase

class TestGetBase(TestCase):

    def setUp(self):
        self.r = Replacer()

    def tearDown(self):
        self.r.restore()
        
    def test_first_call(self):
        self.r.replace('glc.db.Base',None)
        b = getBase()
        self.failUnless(isinstance(getBase(),DeclarativeMeta))

    def test_existing_base(self):
        base = object()
        self.r.replace('glc.db.Base',base)
        self.failUnless(base is getBase())

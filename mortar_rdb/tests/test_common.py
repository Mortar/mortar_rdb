from mortar_rdb.mixins.common import Common
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer
from testfixtures import compare
from unittest import TestCase

class TestUtility(TestCase):

    def setUp(self):
        self.Base = declarative_base()
        
    def test_db_args(self):
        class Model(self.Base,Common):
            id = Column('id', Integer, primary_key=True)
        compare(Model.__table__.kwargs,{'mysql_engine': 'InnoDB'})

    def test_table_name(self):
        class Model(self.Base,Common):
            id = Column('id', Integer, primary_key=True)
        compare(Model.__table__.name,'model')

    def test_table_name_camel(self):
        class SomeModel(self.Base,Common):
            id = Column('id', Integer, primary_key=True)
        compare(SomeModel.__table__.name,'some_model')
    

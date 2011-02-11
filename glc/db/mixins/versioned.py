import datetime

from sqlalchemy.schema import Column
from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.types import  Integer, DateTime

class Versioned(object):
    
    vid =  Column(Integer, primary_key=True)
    valid_from = Column(DateTime(), nullable=False, index=True, default=datetime.datetime.now)
    valid_to = Column(DateTime(), index=True)

    @classmethod
    def valid_on(cls,on_date=None):
        if on_date is None:
            on_date=datetime.datetime.now()
        return and_(cls.valid_from <= on_date,
                    or_(cls.valid_to > on_date,
                        cls.valid_to == None))

    valid = valid_on

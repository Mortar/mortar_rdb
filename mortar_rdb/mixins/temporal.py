import datetime

from sqlalchemy import bindparam
from sqlalchemy.orm.interfaces import MapperOption, MapperExtension
from sqlalchemy.schema import Column
from sqlalchemy.sql.expression import and_
from sqlalchemy.types import  Integer, DateTime

class TemporalExtension(MapperExtension):

    def before_insert(self,mapper,conn,instance):
        table = mapper.mapped_table
        start = instance.value_from
        end = instance.value_to
        if end is None:
            query = (table.c.value_to==None) | (table.c.value_to > start)
        elif start is None:
            raise ValueError((
                'Attempt to insert %r (value_from=%r,value_to=%r) '
                'where value_from is None'
                )%(instance,start,end))
            
        else:
            if end<=start:
                raise ValueError((
                    'Attempt to insert %r (value_from=%r,value_to=%r) '
                    'where value_to is not greater than value_from'
                    )%(instance,start,end))

            query = (
                ((start >= table.c.value_from) & (start < table.c.value_to)) |
                ((start <= table.c.value_from) & (
                    (end > table.c.value_to)|(table.c.value_to==None))
                 )
                )

        if conn.scalar(table.select(query).count()):
            bad = []
            for row in conn.execute(table.select(query)):
                bad.append(repr(dict(row)))
            raise ValueError(
                'Attempt to insert %r (value_from=%r,value_to=%r) which overlaps with:\n%s'%(
                    instance,start,end,'\n'.join(bad)
                    ))

class Temporal(object):

    __mapper_args__ = dict(
        extension=TemporalExtension()
        )
    
    id =  Column(Integer, primary_key=True)
    ref =  Column(Integer,index=True)
    value_from = Column(DateTime, nullable=False, index=True)
    value_to = Column(DateTime, nullable=True, index=True)

    @classmethod
    def value_at(cls,on_date=None):
        if on_date is None:
            on_date=datetime.datetime.now()
        return (cls.value_from <= on_date) & (
                    (cls.value_to > on_date) |
                    (cls.value_to == None)
                    )

    current = value_at
    
class ValueAt(MapperOption):
    
    propagate_to_loaders = True
    
    def __init__(self, temporal_date=None):
        if temporal_date is None:
            temporal_date=datetime.datetime.now()
        self.temporal_date = temporal_date
    
    def process_query_conditionally(self, query):
        """process query during a lazyload"""
        
        query._params = query._params.union(dict(
                temporal_date=self.temporal_date,
            ))
        
    def process_query(self, query):
        """process query during a primary user query"""

        # apply bindparam values
        self.process_query_conditionally(query)

        # any existing critereon
        filter_crit = []
        if query._criterion is not None:
            filter_crit.append(query._criterion)

        # process all mappers
        for e in query._mapper_entities:
            cls = e.entity_zero.class_
            filter_crit.append(cls.value_at(
                    bindparam("temporal_date")
                    ))

        query._criterion = and_(*filter_crit)

Current=ValueAt

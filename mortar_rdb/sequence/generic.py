# Copyright (c) 2011-2013 Simplistix Ltd, 2015 Chris Withers
# See license.txt for license details.
from mortar_rdb.controlled import Source
from pkg_resources import resource_filename
from sqlalchemy import MetaData, Table, Column, String, Integer
from sqlalchemy.sql.expression import select

metadata = MetaData()

sequences = Table(
    'sequences', metadata,
    Column('name', String(20), primary_key=True),
    Column('current', Integer(), default=0),
    mysql_engine='InnoDB',
    )

class SequenceImplementation:
    """
    A sequence implementation that uses a table in the database with
    one row for each named sequence.
    """

    def __init__(self,name,session):
        self.name = name
        if not session.scalar(sequences.count(sequences.c.name==name)):
            session.execute(sequences.insert(dict(name=name)))

    def next(self,session):
        """
        Return the next integer in the sequence using the
        :class:`~sqlalchemy.orm.session.Session` provided.

        .. warning:: The current implementation will lock the row (or
          table, depending on which database you use) for the sequence
          in question. This could conceivably cause contention
          problems if more than one connection is trying to generate
          integers from the sequence at one time.
        """
        # potential optimisation:
        # use our own connection and transaction here,
        # the sequence doesn't need to be consistent with the
        # rest of the stuff going on, it just needs to make
        # sure no number is used more than once.
        
        # update row
        session.execute(sequences.update(
                sequences.c.name==self.name,
                {sequences.c.current:sequences.c.current+1}
                ))
        # return new value
        r = session.scalar(
            select([sequences.c.current],
                   sequences.c.name==self.name)
            )
        return r

source = Source(sequences)

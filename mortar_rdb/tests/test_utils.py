from mortar_rdb import getSession
from mortar_rdb.testing import registerSession
from mortar_rdb.utils import tometadata
from sqlalchemy import (
    Integer, String, UniqueConstraint, 
    CheckConstraint, ForeignKey, MetaData, Sequence, 
    ForeignKeyConstraint, ColumnDefault, Index,
    Table, Column
    )
from testfixtures import compare, ShouldRaise
from unittest import TestCase

import pickle

# largely taken from the relevant bits of sqlalchemy's test.engine.test_metadata
class TestToMetadataDBTests(TestCase):

    def setUp(self):
        self.meta = MetaData()

        self.table = Table('mytable', self.meta,
            Column('myid', Integer, Sequence('foo_id_seq'), primary_key=True),
            Column('name', String(40), nullable=True),
            Column('foo', String(40), nullable=False, server_default='x',
                                                        server_onupdate='q'),
            Column('bar', String(40), nullable=False, default='y',
                                                        onupdate='z'),
            Column('description', String(30),
                                    CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            mysql_engine='InnoDB'
        )

        self.table2 = Table('othertable', self.meta,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
            Column('myid', Integer, 
                        ForeignKey('mytable.myid'),
                    ),
            mysql_engine='InnoDB'
            )

        registerSession(metadata=self.meta)

    def _check(self,table_c,table2_c,has_constraints,reflect):
        assert self.table is not table_c
        assert self.table.primary_key is not table_c.primary_key
        assert list(table2_c.c.myid.foreign_keys)[0].column \
            is table_c.c.myid
        assert list(table2_c.c.myid.foreign_keys)[0].column \
            is not self.table.c.myid
        assert 'x' in str(table_c.c.foo.server_default.arg)
        if not reflect:
            assert isinstance(table_c.c.myid.default, Sequence)
            assert str(table_c.c.foo.server_onupdate.arg) == 'q'
            assert str(table_c.c.bar.default.arg) == 'y'
            assert getattr(table_c.c.bar.onupdate.arg, 'arg',
                           table_c.c.bar.onupdate.arg) == 'z'
            assert isinstance(table2_c.c.id.default, Sequence)

        # constraints dont get reflected for any dialect right
        # now

        if has_constraints:
            for c in table_c.c.description.constraints:
                if isinstance(c, CheckConstraint):
                    break
            else:
                assert False
            assert str(c.sqltext) == "description='hi'"
            for c in table_c.constraints:
                if isinstance(c, UniqueConstraint):
                    break
            else:
                assert False
            assert c.columns.contains_column(table_c.c.name)
            assert not c.columns.contains_column(self.table.c.name)
    
    def test_to_metadata(self):
        meta2 = MetaData()
        table_c = tometadata(self.table,meta2)
        table2_c = tometadata(self.table2,meta2)
        return self._check(table_c, table2_c, True, False)

    def test_pickle(self):
        self.meta.bind = getSession().bind
        meta2 = pickle.loads(pickle.dumps(self.meta))
        assert meta2.bind is None
        meta3 = pickle.loads(pickle.dumps(meta2))
        return self._check(
            meta2.tables['mytable'], meta2.tables['othertable'],
            True, False)

    def test_pickle_via_reflect(self):
        # this is the most common use case, pickling the results of a
        # database reflection
        meta2 = MetaData(bind=getSession().bind)
        t1 = Table('mytable', meta2, autoload=True)
        t2 = Table('othertable', meta2, autoload=True)
        meta3 = pickle.loads(pickle.dumps(meta2))
        assert meta3.bind is None
        assert meta3.tables['mytable'] is not t1
        return self._check(
            meta3.tables['mytable'], meta3.tables['othertable'],
            False, True
            )

class TestToMetadata(TestCase):

    def test_tometadata_with_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                            CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid')),
            )

        meta2 = MetaData()
        table_c = tometadata(table,meta2, schema='someschema')
        table2_c = tometadata(table2,meta2, schema='someschema')

        compare(str(table_c.join(table2_c).onclause),
                str(table_c.c.myid == table2_c.c.myid))
        compare(str(table_c.join(table2_c).onclause),
                'someschema.mytable.myid = someschema.othertable.myid')

    def test_tometadata_kwargs(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            mysql_engine='InnoDB',
        )

        meta2 = MetaData()
        table_c = tometadata(table,meta2)

        compare(table.kwargs,table_c.kwargs)

    def test_tometadata_indexes(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('id', Integer, primary_key=True),
            Column('data1', Integer, index=True),
            Column('data2', Integer),
        )
        Index('multi',table.c.data1,table.c.data2),
        
        meta2 = MetaData()
        table_c = tometadata(table,meta2)

        def _get_key(i):
            entry = [i.name,i.unique]
            entry.extend(sorted(i.kwargs.items()))
            entry.extend(i.columns.keys())
            return entry

        table_indexes = [_get_key(i) for i in table.indexes]
        table_indexes.sort()
        table_c_indexes = [_get_key(i) for i in table_c.indexes]
        table_c_indexes.sort()
            
        compare(table_indexes,table_c_indexes)

    def test_tometadata_already_there(self):
        
        meta1 = MetaData()
        table1 = Table('mytable', meta1,
            Column('myid', Integer, primary_key=True),
        )
        meta2 = MetaData()
        table2 = Table('mytable', meta2,
            Column('yourid', Integer, primary_key=True),
        )

        meta3 = MetaData()
        
        tometadata(table1,meta3)
        with ShouldRaise(KeyError("'mytable' is already in MetaData(None)")):
            tometadata(table2,meta3)

    def test_tometadata_default_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                        CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            schema='myschema',
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('myschema.mytable.myid')),
            schema='myschema',
            )

        meta2 = MetaData()
        table_c = tometadata(table,meta2)
        table2_c = tometadata(table2,meta2)

        compare(str(table_c.join(table2_c).onclause),
                str(table_c.c.myid == table2_c.c.myid))
        compare(str(table_c.join(table2_c).onclause),
                'myschema.mytable.myid = myschema.othertable.myid')

    def test_tometadata_strip_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                        CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid')),
            )

        meta2 = MetaData()
        table_c = tometadata(table,meta2, schema=None)
        table2_c = tometadata(table2,meta2, schema=None)

        compare(str(table_c.join(table2_c).onclause),
                str(table_c.c.myid== table2_c.c.myid))
        compare(str(table_c.join(table2_c).onclause),
                'mytable.myid = othertable.myid')

    

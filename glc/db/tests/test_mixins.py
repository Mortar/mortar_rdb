from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy import Column,Integer,Index
from testfixtures import compare
from unittest import TestCase

class TestMixins(TestCase):

    def test_doing_indexes_in_a_mixin(self):

        Base = declarative_base()

        class MyMeta(DeclarativeMeta):

            def __init__(cls,*args,**kw):
                if getattr(cls,'_decl_class_registry',None) is None:
                    return
                super(MyMeta,cls).__init__(*args,**kw)
                cols = cls.__table__.c
                # Index creation done here
                Index('test',cols.a,cols.b)

        class MyMixin(object):
            __metaclass__=MyMeta
            a =  Column(Integer)
            b =  Column(Integer)

        class MyModel(Base,MyMixin):
            __tablename__ = 'awooooga'
            c =  Column(Integer,primary_key=True)

        compare(['a','b','c'],MyModel.__table__.c.keys())
        index = tuple(MyModel.__table__.indexes)[0]
        compare(['a','b'],index.columns.keys())

    def test_multiple_mixins_with_metaclasses(self):

        Base = declarative_base()

        class MyMeta1(DeclarativeMeta):

            def __init__(cls,*args,**kw):
                if getattr(cls,'_decl_class_registry',None) is None:
                    return
                super(MyMeta1,cls).__init__(*args,**kw)
                cols = cls.__table__.c
                Index('test1',cols.a,cols.b)

        class MyMixin1(object):
            __metaclass__=MyMeta1
            a =  Column(Integer)
            b =  Column(Integer)

        class MyMeta2(DeclarativeMeta):

            def __init__(cls,*args,**kw):
                if getattr(cls,'_decl_class_registry',None) is None:
                    return
                super(MyMeta2,cls).__init__(*args,**kw)
                cols = cls.__table__.c
                Index('test2',cols.c,cols.d)

        class MyMixin2(object):
            __metaclass__=MyMeta2
            c =  Column(Integer)
            d =  Column(Integer)

        class CombinedMeta(MyMeta1,MyMeta2):
            # This is needed to successfully combine
            # two mixins which both have metaclasses
            pass
        
        class MyModel(Base,MyMixin1,MyMixin2):
            __tablename__ = 'awooooga'
            __metaclass__ = CombinedMeta
            z =  Column(Integer,primary_key=True)

        compare(['a','b','c','d','z'],MyModel.__table__.c.keys())

        indexes = {}
        for i in MyModel.__table__.indexes:
            indexes[i.name]=i

        compare(['a','b'],indexes['test1'].columns.keys())
        compare(['c','d'],indexes['test2'].columns.keys())

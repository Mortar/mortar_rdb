from datetime import datetime
from glc.db import getSession
from glc.db.controlled import Config
from glc.db.mixins.temporal import Temporal,ValueAt,Current
from glc.db.sequence import registerSequence, getSequence
from glc.db.sequence.generic import source
from glc.db.testing import registerSession
from glc.testing.component import TestComponents
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.sql.expression import and_
from sqlalchemy.types import Integer, String, Numeric
from testfixtures import compare
from unittest import TestCase

import transaction

class Tests(TestCase):

    def setUp(self):
        self.datetime = datetime
        self.components = TestComponents()
        self.Base = declarative_base()

        # names change over time...
        class Name(self.Base,Temporal):
            __tablename__ = 'name'
            name = Column(String(50))

            def __repr__(self):
                return '<Name:%s>'%self.name

        self.Name= Name

        # a temporal user, bit lame but I can't think
        # of another example...
        class User(self.Base,Temporal):
            __tablename__ = 'user'
            ref =  Column(Integer)
            name =  Column(ForeignKey(Name.ref))
            job_title = Column(String(50))

        self.User = User

        # a non-temporal model
        class Fish(self.Base):
            __tablename__ = 'fish'
            id = Column(Integer, primary_key=True) 
            name =  Column(ForeignKey(Name.ref))
            species = Column(String(50))

        self.Fish = Fish

        registerSession(config=Config(source),metadata=self.Base.metadata,)
        with transaction:
            session = getSession()
            registerSequence('name',session)
            registerSequence('user',session)
        self.t = transaction.get()

    def tearDown(self):
        self.t.abort()
        self.components.uninstall()

    def _make_names(self):
        # some names
        session = getSession()
        session.add(self.Name(
                ref=1,
                value_from=self.datetime(2000,1,1),
                value_to=self.datetime(2001,1,1),
                name = 'Name 1-1'
                ))
        session.add(self.Name(
                ref=1,
                value_from=self.datetime(2001,1,1),
                value_to=self.datetime(2002,1,1),
                name = 'Name 1-2'
                ))
        session.add(self.Name(
                ref=1,
                value_from=self.datetime(2002,1,1),
                name = 'Name 1-3'
                ))
        session.flush()
        
    def test_make_one(self):        
        name_seq = getSequence('name')
        session = getSession()
        session.add(self.Name(
                ref=name_seq.next(session),
                value_from=self.datetime(2000,1,1),
                value_to=self.datetime(2001,1,1),
                name = 'Name 1-1'
                ))
        session.flush()
        compare([{
                    u'value_from': self.datetime(2000, 1, 1, 0, 0),
                    u'ref': 1,
                    u'id': 1,
                    u'name': u'Name 1-1',
                    u'value_to': self.datetime(2001, 1, 1, 0, 0),
                    }],
                map(dict,session.execute(self.Name.__table__.select())))

    def test_value_at_mapper_option(self):
        self._make_names()
        session = getSession()
        
        compare([],
                session.query(self.Name).options(
                ValueAt(self.datetime(1999,2,3))
                ).all())
        
        c = session.query(self.Name).options(
            ValueAt(self.datetime(2000,2,3))
            ).one()
        compare('Name 1-1',c.name)

        # boundary
        c = session.query(self.Name).options(
            ValueAt(self.datetime(2001,1,1))
            ).one()
        compare('Name 1-2',c.name)
        
        c = session.query(self.Name).options(
            ValueAt(self.datetime(2001,2,3))
            ).one()
        compare('Name 1-2',c.name)
        
        c = session.query(self.Name).options(
            ValueAt(self.datetime(2010,1,1))
            ).one()
        compare('Name 1-3',c.name)
    
        c = session.query(self.Name).options(Current()).one()
        compare('Name 1-3',c.name)
        
    def test_value_at_clause(self):
        self._make_names()
        session = getSession()
        
        compare([],
                session.query(self.Name).filter(
                self.Name.value_at(
                    self.datetime(1999,2,3)
                    )).all())
        
        c = session.query(self.Name).filter(
            self.Name.value_at(
                self.datetime(2000,2,3)
                )).one()
        compare('Name 1-1',c.name)
        
        c = session.query(self.Name).filter(
            self.Name.value_at(
                self.datetime(2001,2,3)
                )).one()
        compare('Name 1-2',c.name)
        
        c = session.query(self.Name).filter(
            self.Name.value_at(
                self.datetime(2010,1,1)
                )).one()
        compare('Name 1-3',c.name)
    
        c = session.query(self.Name).filter(
            self.Name.current()
            ).one()
        compare('Name 1-3',c.name)

    def _check_valid(self,existing=(),new=(),exception=None):
        # both existing and new should be sequences of two-tuples
        # of the form (value_from,value_to)
        session = getSession()
        for value_from,value_to in existing:
            session.add(self.Name(
                ref=1,
                value_from=value_from,
                value_to=value_to,
                name = 'Name'
                ))

        session.flush()
        try:
            for value_from,value_to in new:
                session.add(self.Name(
                    ref=1,
                    value_from=value_from,
                    value_to=value_to,
                    name = 'Name'
                    ))
            session.flush()
        except ValueError,v:
            if exception is None:
                raise
            compare(exception,v.args[0])
        else:
            if exception:
                self.fail("No exception raised!")
                
    def test_invalid_new_instance(self):
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2010,1,1))],
            new=[(self.datetime(2002,1,1),
                  self.datetime(2003,1,1))],
            exception=(
                'Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, 1, 0, 0),'
                'value_to=datetime.datetime(2003, 1, 1, 0, 0)) which overlaps with:\n'
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2010, 1, 1, 0, 0)}"
                ))

    def test_invalid_1(self):
        # existing:     |---->
        #      new:  |------->
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       None)],
            new=[(self.datetime(2002,1,1),
                  None)],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, "
                "1, 0, 0),value_to=None) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': None}"
                ))

    def test_invalid_2(self):
        # existing:  |------->
        #      new:     |---->
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       None)],
            new=[(self.datetime(2002,1,1),
                  None)],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, "
                "1, 0, 0),value_to=None) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': None}"
                ))

    def test_invalid_3(self):
        # existing:  |------->
        #      new:  |------->
        self._check_valid(
            existing=[(self.datetime(2002,1,1),
                       None)],
            new=[(self.datetime(2002,1,1),
                  None)],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, "
                "1, 0, 0),value_to=None) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2002, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': None}"
                ))

    def test_invalid_4(self):
        # existing:  |-------|
        #      new:  |-------|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2002,1,1))],
            new=[(self.datetime(2001,1,1),
                  self.datetime(2002,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2001, 1, "
                "1, 0, 0),value_to=datetime.datetime(2002, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2002, 1, 1, 0, 0)}"
                ))
    
    def test_invalid_5(self):
        # existing:  |-------|
        #      new:  |-----|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2001,1,1),
                  self.datetime(2002,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2001, 1, "
                "1, 0, 0),value_to=datetime.datetime(2002, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2003, 1, 1, 0, 0)}"
                ))
    
    def test_invalid_6(self):
        # existing:  |-------|
        #      new:    |-----|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2002,1,1),
                  self.datetime(2003,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, "
                "1, 0, 0),value_to=datetime.datetime(2003, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2003, 1, 1, 0, 0)}"
                ))
    
    def test_invalid_7(self):
        # existing:  |-------|
        #      new:    |---|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2004,1,1))],
            new=[(self.datetime(2002,1,1),
                  self.datetime(2003,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2002, 1, "
                "1, 0, 0),value_to=datetime.datetime(2003, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2004, 1, 1, 0, 0)}"
                ))
    
    def test_invalid_8(self):
        # existing:    |---|
        #      new:  |-------|
        self._check_valid(
            existing=[(self.datetime(2002,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2001,1,1),
                  self.datetime(2004,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2001, 1, "
                "1, 0, 0),value_to=datetime.datetime(2004, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2002, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2003, 1, 1, 0, 0)}"
                ))
    
    def test_invalid_9(self):
        #      new:  |<------|
        self._check_valid(
            new=[(self.datetime(2004,1,1),
                  self.datetime(2001,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2004, 1, "
                "1, 0, 0),value_to=datetime.datetime(2001, 1, 1, 0, 0)) where value_to "
                "is not greater than value_from"
                ))
    
    def test_invalid_10(self):
        #      new:  <-------|
        self._check_valid(
            new=[(None,
                  self.datetime(2004,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=None,value_to=datetime."
                "datetime(2004, 1, 1, 0, 0)) where value_from is None"
                ))
    
    def test_invalid_11(self):
        # existing:     |---->
        #      new:  |-------|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       None)],
            new=[(self.datetime(2000,1,1),
                 self.datetime(2002,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2000, 1, "
                "1, 0, 0),value_to=datetime.datetime(2002, 1, 1, 0, 0)) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2001, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': None}"
                ))

    def test_invalid_12(self):
        # existing:  |-------|
        #      new:     |---->
        self._check_valid(
            existing=[(self.datetime(2000,1,1),
                       self.datetime(2002,1,1))],
            new=[(self.datetime(2001,1,1),
                  None)],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2001, 1, "
                "1, 0, 0),value_to=None) which overlaps with:\n"
                "{u'value_from': datetime.datetime(2000, 1, 1, 0, 0), u'ref': 1, u'id': 1, "
                "u'name': u'Name', u'value_to': datetime.datetime(2002, 1, 1, 0, 0)}"
                ))

    def test_invalid_13(self):
        #      new:  |
        self._check_valid(
            new=[(self.datetime(2004,1,1),
                  self.datetime(2004,1,1))],
            exception=(
                "Attempt to insert <Name:Name> (value_from=datetime.datetime(2004, "
                "1, 1, 0, 0),value_to=datetime.datetime(2004, 1, 1, 0, 0)) where "
                "value_to is not greater than value_from"
                ))
    
    def test_ok_1(self):
        # existing:  |---|
        #      new:      |--->
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2002,1,1))],
            new=[(self.datetime(2002,1,1),
                  None)],
            )
    
    def test_ok_2(self):
        # existing:  |---|
        #      new:      |---|
        self._check_valid(
            existing=[(self.datetime(2001,1,1),
                       self.datetime(2002,1,1))],
            new=[(self.datetime(2002,1,1),
                  self.datetime(2003,1,1))],
            )
    
    def test_ok_3(self):
        # existing:      |---|
        #      new:  |---|
        self._check_valid(
            existing=[(self.datetime(2002,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2001,1,1),
                  self.datetime(2002,1,1))],
            )
    
    def test_ok_4(self):
        # existing:        |---|
        #      new:  |---|
        self._check_valid(
            existing=[(self.datetime(2002,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2000,1,1),
                  self.datetime(2001,1,1))],
            )
    
    def test_ok_5(self):
        # existing:  |---|
        #      new:        |---|
        self._check_valid(
            existing=[(self.datetime(2000,1,1),
                       self.datetime(2001,1,1))],
            new=[(self.datetime(2002,1,1),
                  self.datetime(2003,1,1))],
            )
    
    def test_ok_6(self):
        # existing:  |---|   |---|
        #      new:      |---|
        self._check_valid(
            existing=[(self.datetime(2000,1,1),
                       self.datetime(2001,1,1)),
                      (self.datetime(2002,1,1),
                       self.datetime(2003,1,1))],
            new=[(self.datetime(2001,1,1),
                  self.datetime(2002,1,1))],
            )
    
    def test_invalid_update_instance(self):
        # add
        session = getSession()
        obj = self.Name(
            ref=1,
            value_from=self.datetime(2000,1,1),
            value_to=self.datetime(2001,1,1),
            name = 'Name'
            )
        session.add(obj)
        
        # change to be invalid
        # flush, which should raise exception


    def test_ok_update_instance(self):
        pass

    def test_ok_update_instance_no_change(self):
        pass

    def test_ok_update_instance_change_to_same_value(self):
        pass

    def test_indexing(self):
        pass

    # def test_use_case_1(self):
    #     # find a company by its current name
    #     # what was it called in 2000?
    #     pass

    # def test_use_case_2(self):
    #     # tell me the name of Person 1
    #     # and the name of the company they are
    #     # associated with on 3 Feb 2001
    #     p,c = getSession().query(self.Person,self.Company).\
    #         options(ValueOn(self.datetime(2001,2,3))
    #         ).\
    #         filter(and_(
    #             self.Person.ref==1,
    #             self.Person.company_ref==self.Company.ref,
    #             )).one()

    #     compare('Person, name 2',p.name)
    #     compare('Company 1, name 2',c.name)


    #     name_seq = getSequence('name')
    #     user_seq = getSequence('user')
        
    #     with transaction:
    #         session = getSession()
    #         # some names
    #         session.add(Name(
    #                 ref=1,
    #                 value_from=self.datetime(2000,1,1),
    #                 value_to=self.datetime(2001,1,1),
    #                 name = 'Name 1-1'
    #                 ))
    #         session.add(Name(
    #                 ref=1,
    #                 value_from=self.datetime(2001,1,1),
    #                 value_to=self.datetime(2002,1,1),
    #                 name = 'Name 1-2'
    #                 ))
    #         session.add(Name(
    #                 ref=1,
    #                 value_from=self.datetime(2002,1,1),
    #                 name = 'Name 1-3'
    #                 ))
    #         # another company
    #         session.add(Company(
    #                 ref=2,
    #                 value_from=self.datetime(1990,1,1),
    #                 name = 'Company 2'
    #                 ))
    #         # a person
    #         session.add(Person(
    #                 ref=1,
    #                 value_from=self.datetime(2000,1,1),
    #                 value_to=self.datetime(2001,1,1),
    #                 name = 'Person, name 1',
    #                 company_ref = 1,
    #                 ))
    #         session.add(Person(
    #                 ref=1,
    #                 value_from=self.datetime(2001,1,1),
    #                 name = 'Person, name 2',
    #                 company_ref = 1,
    #                 ))

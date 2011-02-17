if False:
    from mortar_rdb import getSession
    from mortar_rdb.mixins.versioned import Versioned
    from mortar_rdb.testing import registerSession
    from testfixtures.components import TestComponents
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.schema import Column, ForeignKey
    from sqlalchemy.sql.expression import and_
    from sqlalchemy.types import Integer, String
    from testfixtures import compare,test_datetime,Comparison as C
    from unittest import TestCase

    import transaction

    class Base(TestCase):

        def setUp(self):
            self.datetime = test_datetime()
            self.datetime.set(2003,1,1)
            self.components = TestComponents()
            self.Base = declarative_base()

            class Company(self.Base,Versioned):
                __tablename__ = 'company'
                ref =  Column(Integer)
                name = Column(String(50))

            self.Company = Company

            class Person(self.Base,Versioned):
                __tablename__ = 'person'
                ref =  Column(Integer)
                name = Column(String(50))
                company_ref =  Column(ForeignKey(Company.ref))
                address_id =  Column(ForeignKey('address.id'))

            self.Person = Person

            class Address(self.Base):
                __tablename__ = 'address'
                # we don't need audit trail for address changes,
                # apparently ;-)
                id = Column(Integer, primary_key=True) 
                value = Column(String(50)) # all in one field :-P

            self.Address = Address

            registerSession(self.Base)

        def tearDown(self):
            self.components.uninstall()

        def _makeOne(self,klass,**kw):
            with transaction:
                session = getSession()
                instance = klass(**kw)
                session.add(instance)
            return instance

    class TestCRUD(Base):

        def test_create(self):

            with transaction:
                session = getSession()
                company = self.Company(name='Company 1, name 1')
                # no values at this point
                compare(company.id,None)
                compare(company.ref,None)
                compare(company.valid_from,None)
                compare(company.valid_to,None)
                # add to session
                session.add(company)

            # now that we've committed, we do have the attributes
            compare(company.id,1)
            compare(company.ref,1)
            compare(company.valid_from,self.datetime(2001,1,1))
            compare(company.valid_to,None)

            # and we can get it back!
            session = getSession()
            retrieved_company = session.query(self.Company).one()
            compare(retrieved_company.id,1)
            compare(retrieved_company.ref,1)
            compare(retrieved_company.valid_from,self.datetime(2001,1,1))
            compare(retrieved_company.valid_to,None)

        def test_update(self):
            self._makeOne(self.Company,name='Company 1, name 1')
            with transaction:
                session = getSession()
                company = session.query(self.Company).one()
                company.name='Company 1, name 2'

            compare([C(self.Company,
                       id=1,
                       ref=1,
                       valid_from=self.datetime(2001,1,1),
                       valid_to=self.datetime(2001,1,1,0,0,10),
                       name='Company 1, name 1',
                       strict=False),
                     C(self.Company,
                       id=2,
                       ref=1,
                       valid_from=self.datetime(2001,1,1,0,0,10),
                       valid_to=None,
                       name='Company 1, name 1',
                       strict=False),
                    ],getSession().query(self.Company).all())

        def test_set_but_not_changed(self):
            self._makeOne(self.Company,name='Company 1, name 1')
            with transaction:
                session = getSession()
                company = session.query(self.Company).one()
                company.name=company.name

            compare([C(self.Company,
                       id=1,
                       ref=1,
                       valid_from=self.datetime(2001,1,1),
                       valid_to=None,
                       name='Company 1, name 1',
                       strict=False),
                    ],getSession().query(self.Company).all())

        def test_delete(self):
            self._makeOne(self.Company,name='GLC')
            with transaction:
                session = getSession()
                company = session.query(self.Company).one()
                session.delete(company)

            compare([C(self.Company,
                       id=1,
                       ref=1,
                       valid_from=self.datetime(2001,1,1),
                       valid_to=self.datetime(2001,1,1,0,0,10),
                       name='Company 1, name 1',
                       strict=False),
                    ],getSession().query(self.Company).all())

            # paranoid check
            session = getSession()
            # should raise an exception...
            session.query(self.Company).options(Valid()).one()

        def test_add_history(self):
            # simulated creating abunch of history
            # in one go
            with transaction:
                session = getSession()

            pass

        def test_bulk_delete(self):
            pass

    class TestBadAdding(Base):

        def add_overlapping_(self):
            pass

        def add_overlapping_(self):
            pass

        def add_overlapping_(self):
            pass

    class TestSearching(Base):

        def something(self):

            with transaction:
                session = getSession()
                # one company
                session.add(Company(
                        ref=1,
                        value_from=self.datetime(2000,1,1),
                        value_to=self.datetime(2001,1,1),
                        name = 'Company 1, name 1'
                        ))
                session.add(Company(
                        ref=1,
                        value_from=self.datetime(2001,1,1),
                        value_to=self.datetime(2002,1,1),
                        name = 'Company 1, name 2'
                        ))
                session.add(Company(
                        ref=1,
                        value_from=self.datetime(2002,1,1),
                        name = 'Company 1, name 3'
                        ))
                # another company
                session.add(Company(
                        ref=2,
                        value_from=self.datetime(1990,1,1),
                        name = 'Company 2'
                        ))
                # a person
                session.add(Person(
                        ref=1,
                        value_from=self.datetime(2000,1,1),
                        value_to=self.datetime(2001,1,1),
                        name = 'Person, name 1',
                        company_ref = 1,
                        ))
                session.add(Person(
                        ref=1,
                        value_from=self.datetime(2001,1,1),
                        name = 'Person, name 2',
                        company_ref = 1,
                        ))

        def test_value_on(self):
            return
            # tell me what the company name was on 3 Feb 2001
            c = getSession().query(self.Company).filter(and_(
                self.Company.ref==1,
                self.Company.value_on(self.datetime(2001,2,3)),
                )).one()
            compare('Company 1, name 2',c.name)

        def test_value_on_options(self):
            return
            # tell me what the company name was on 3 Feb 2001
            c = getSession().query(self.Company).\
                options(
                ValueOn(self.datetime(2001,2,3))
                ).\
                filter(
                    self.Company.ref==1,
                    ).one()
            compare('Company 1, name 2',c.name)

        def test_current(self):
            return
            # tell me what the current company name is
            c = getSession().query(self.Company).filter(and_(
                self.Company.ref==1,
                self.Company.current(),
                )).one()
            compare('Company 1, name 3',c.name)

        def test_invalid_new_instance(self):
            pass

        def test_use_case_1(self):
            return
            # find a company by its current name
            # what was it called in 2000?
            session = getSession()
            current = self.session.query(self.Company).options(Valid()).filter(
                self.Company.name=='Company'
                ).subquery()



            pass

        def test_use_case_2(self):
            return
            # tell me the name of Person 1
            # and the name of the company they are
            # associated with on 3 Feb 2001
            p,c = getSession().query(self.Person,self.Company).\
                options(ValueOn(self.datetime(2001,2,3))
                ).\
                filter(and_(
                    self.Person.ref==1,
                    self.Person.company_ref==self.Company.ref,
                    )).one()

            compare('Person, name 2',p.name)
            compare('Company 1, name 2',c.name)

    

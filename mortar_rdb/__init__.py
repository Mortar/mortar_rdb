# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.

from logging import getLogger
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base as sa_declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.interfaces import SessionExtension
from sqlalchemy.schema import (
    MetaData,
    Table,
    DropTable,
    ForeignKeyConstraint,
    DropConstraint,
    )
from zope.component import getSiteManager
from zope.sqlalchemy import ZopeTransactionExtension
from zope.sqlalchemy.datamanager import STATUS_CHANGED

from .interfaces import ISession

logger = getLogger('mortar_rdb')

def register_session(url=None,
                    name=u'',
                    engine=None,
                    echo=None,
                    transactional=True,
                    scoped=True,
                    extension=None,
                    twophase=True):
    """
    Create a :class:`~sqlalchemy.orm.session.Session` class and
    register it for later use.

    Generally, you'll only need to pass in a :mod:`SQLAlchemy`
    connection URL. If you want to register multiple sessions for a
    particular application, then you should name them.
    If you want to provide specific engine configuration, then you can
    pass in an :class:`~sqlalchemy.engine.base.Engine` instance.
    In that case, you must not pass in a URL.

    :param echo: If `True`, then all SQL will be echoed to the python
      logging framework. This option cannot be specified if you pass in
      an engine.

    :param scoped: If `True`, then :func:`get_session` will return a distinct
      session for each thread that it is called from but, within that thread,
      it will always return the same session. If it is `False`, every call
      to :func:`get_session` will return a new session.
    
    :param transactional:

      If `True`, a :mod:`SQLAlchemy` extension will
      be used that that enables the :mod:`transaction` package to
      manage the lifecycle of the SQLAlchemy session (eg:
      :meth:`~sqlalchemy.orm.session.Session.begin`/:meth:`~sqlalchemy.orm.session.Session.commit`/:meth:`~sqlalchemy.orm.session.Session.rollback`).
      This can only be done when scoped sessions are used.

      If `False`, you will need to make sure you call
      :meth:`~sqlalchemy.orm.session.Session.begin`/:meth:`~sqlalchemy.orm.session.Session.commit`/:meth:`~sqlalchemy.orm.session.Session.rollback`,
      as appropriate, yourself. 

    :param extension: An optional :class:`~sqlalchemy.orm.interfaces.SessionExtension`
      or sequence of :class:`~sqlalchemy.orm.interfaces.SessionExtension`
      objects to be used with the session that is registered.

    :param twophase: By default two-phase transactions are used where
      supported by the underlying database. Where this causes problems,
      single-phase transactions can be used for all engines by passing this
      parameter as `False`.

    """
    if (engine and url) or not (engine or url):
        raise TypeError('Must specify engine or url, but not both')

    if transactional and not scoped:
        raise TypeError(
            'Transactions can only be managed when using scoped sessions'
            )
        
    if engine:
        if echo:
            raise TypeError('Cannot specify echo if an engine is passed')
    else:
        engine = create_engine(url, echo=echo)

    url = str(engine.url)
    if engine.url.password is not None:
        url = url.replace(engine.url.password, '<password>')
    logger.info('Registering session for %r with name %r',
                url, name)

    params = dict(
            bind = engine,
            autoflush=True,
            autocommit=False,
            )

    if extension is None:
        extensions = []
    else:
        extensions = list(extension)

    if transactional:
        extensions.append(
            ZopeTransactionExtension(
            # We want transactions committed regardless of
            # whether or not we use the ORM.
            initial_state=STATUS_CHANGED,
            ))
        if twophase and engine.dialect.name in ('postgresql', 'mysql'):
            params['twophase']=True

    if extensions:
        if len(extensions)==1:
            params['extension']=extensions[0]
        else:
            params['extension']=extensions

    Session = sessionmaker(**params)
    
    if scoped:
        Session = scoped_session(Session)
    
    getSiteManager().registerUtility(
        Session,
        provided=ISession,
        name=name,
        ) 

def drop_tables(engine):
    """
    Drop all the tables in the database attached to by the supplied
    engine.
    
    As many foreign key constraints as possible will be dropped
    first making this quite brutal!
    """
    # from http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DropEverything
    conn = engine.connect()

    inspector = Inspector.from_engine(engine)

    # gather all data first before dropping anything.
    # some DBs lock after things have been dropped in 
    # a transaction.
    metadata = MetaData()

    tbs = []
    for table_name in inspector.get_table_names():
        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            if not fk['name']:
                continue
            fks.append(
                ForeignKeyConstraint((),(),name=fk['name'])
                )
        t = Table(table_name, metadata,*fks)
        tbs.append(t)
        for fkc in fks:
            conn.execute(DropConstraint(fkc, cascade=True))

    for table in tbs:
        conn.execute(DropTable(table))

def get_session(name=u''):
    """
    Return a :class:`~sqlalchemy.orm.session.Session` instance from
    the current registry as registered with the supplied `name`.
    """
    return getSiteManager().getUtility(ISession,name)()

_bases = {}

def declarative_base(**kw):
    """
    Return a :obj:`Base` as would be returned by
    :func:`~sqlalchemy.ext.declarative.declarative_base`.

    Only one :obj:`Base` will exist for each combination of parameters
    that this function is called with. If it is called with the same
    combination of parameters more than once, subsequent calls will
    return the existing :obj:`Base`.

    This method should be used so that even if more than one package
    used by a project defines models, they will all end up in the
    same :class:`~sqlalchemy.schema.MetaData` instance and all have the
    same declarative registry.
    """
    key = tuple(kw.items())
    if key in _bases:
        return _bases[key]
    base = sa_declarative_base(**kw)
    _bases[key] = base
    return base

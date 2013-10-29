# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.
"""
Helpers for unit testing when using :mod:`mortar_rdb`
"""

import os

from . import (
    get_session, drop_tables,
    register_session as real_register_session
    )
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

import mortar_rdb

def register_session(url=None,
                     name=u'',
                     engine=None,
                     echo=False,
                     transactional=True,
                     scoped=True,
                     config=None,
                     metadata=None,
                     extension=None):
    """
    This will create a :class:`~sqlalchemy.orm.session.Session` class for
    testing purposes and register it for later use.
    
    The calling parameters mirror those of :func:`mortar_rdb.register_session`
    but if neither `url` nor `engine` is specified then:

    - The environment will be consulted for a variable called ``DB_URL``.
      If found, that will be used for the `url` parameter.

    - If `url` is still `None`, an implicit `url` of ``sqlite://`` will
      be used.

    If a :class:`~mortar_rdb.controlled.Config` is passed in then, once
    any existing content in the database has been removed, any tables
    controlled by that config will be created.

    If a :class:`~sqlalchemy.schema.MetaData` instance is passed in,
    then all tables within it will be created.

    Unlike the non-testing :class:`~mortar_rdb.register_session`, this will
    also return an instance of the registered session.
    
    .. warning::

      No matter where the `url` or `engine` come from, the entire
      contents of the database they point at will be destroyed!

    """

    if not (url or engine):
        url = os.environ.get('DB_URL')
        if not url:
            # we use a StaticPool so that the in memory databases
            # don't leak between individual tests
            engine = create_engine('sqlite://',
                                   poolclass=StaticPool,
                                   echo=echo)
            # don't confuse the real register_session
            echo = False
        
    real_register_session(
        url,
        name,
        engine,
        echo,
        transactional,
        scoped,
        None,
        extension
        )
    session = get_session(name)
    engine = session.bind
    
    drop_tables(engine)
    
    if config is not None:
        for source in config.sources:
            source.metadata.create_all(engine)

    if metadata is not None:
        metadata.create_all(engine)

    return session

class TestingBase(object):
    """
    This is a helper class that can either be used to make
    :func:`~mortar_rdb.declarative_base` return a new, empty :class:`Base`
    for testing purposes.

    If writing a suite of unit tests, this can be done as follows:

    .. code-block:: python

      from mortar_rdb.testing import TestingBase
      from unittest import TestCase
      
      class YourTestCase(TestCase):

          def setUp(self):
              self.tb = TestingBase()

          def tearDown(self):
              self.tb.restore()

    If you need a fresh :class:`Base` for a short section of Python
    code, :class:`TestingBase` can also be used as a context manager:

    .. code-block:: python
    
      with TestingBase():
          base = declarative_base()
          # your test code here
    """

    def __init__(self):
        self.original = mortar_rdb._bases
        mortar_rdb._bases = {}

    def restore(self):
        mortar_rdb._bases = self.original

    def __enter__(self):
        return self
    
    def __exit__(self,*args):
        self.restore()

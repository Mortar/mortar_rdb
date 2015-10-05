# Copyright (c) 2011-2013 Simplistix Ltd, 2015 Chris Withers
# See license.txt for license details.
from __future__ import print_function
"""
When a database is used, it's essential that code using the database
only interacts with a database that is of the form it expects.
A corollary of that is that it is important to be able to update the
structure of a database from the form expected by one version of the
code to that expected by another version of the code.

:mod:`mortar_rdb.controlled` aims to facilitate this along with providing
a command line harness for creating necessary tables within a
database, emptying out a non-production database and upgrading a
database to a new structure where :mod:`SQLAlchemy` is
used.

Packages, Models and Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:mod:`SQLAlchemy` uses :class:`~sqlalchemy.schema.Table` objects that
are mapped to one or more model classes. These objects are defined
within python packages

Configurations
~~~~~~~~~~~~~~

A single database may contain tables that are defined in more that one
package. For example, an authentication package may contain some
table definitions for storing users and their permissions. That package
may be used by an application which also contains a package that
defines its own tables.

A :class:`~mortar_rdb.controlled.Config` is a way of expressing
which tables should be expected in a database.

It general, it is recommended that a
:class:`~mortar_rdb.controlled.Config` is defined once, in whatever
package 'owns' a particular database. For example, an application may
define a configuration for its own tables and those of any packages on
which it relies, such as the hypothetical authentication package
described above. If another application wants to use this
application's database, it can import the configuration and check that
the database structure matches that expected by the code it is
currently using.
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from inspect import getmembers
from pkgutil import walk_packages
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine.reflection import Inspector
from zope.dottedname.resolve import resolve

import sys

class Source:
    """
    A collection of tables that should have their versioning managed together.
    This usually means they originate from one package.
    
    :param tables:
          A sequence of :class:`~sqlalchemy.schema.Table` objects that
          contain all the tables that will be managed by the
          repository in this Source. 
    """

    def __init__(self, *tables):
        
        self.metadata = MetaData()
        
        for table in tables:
            if not isinstance(table,Table):
                raise TypeError(
                    '%r must be a Table object or a declaratively '
                    'mapped model class.' % (
                        table
                        ))
            table.tometadata(self.metadata)

def scan(package, tables=()):
    """Scan a package or module and return a
    :class:`~mortar_rdb.controlled.Source` containing the tables from any
    declaratively mapped models found, any
    :class:`~sqlalchemy.schema.Table` objects explicitly passed in and
    the :mod:`sqlalchemy-migrate` repository contained within the
    package.

    .. note::
      While the `package` parameter is passed as a string, this will
      be resolved into a module or package object. It is not a
      distribution name, although the two are often very similar.

    :param package:
          A dotted path to the package to be scanned for
          :class:`~sqlalchemy.schema.Table` objects.

    :param tables:
          A sequence of :class:`~sqlalchemy.schema.Table` objects to
          be added to the returned :class:`~mortar_rdb.controlled.Source`.
          Any tables not created as part of declaratively mapping a
          class will need to be passed in using this sequence as
          :func:`~mortar_rdb.controlled.scan` cannot sensibly scan for
          these objects.
    """
    package_ob = resolve(package)
    to_search = [package_ob]
    if hasattr(package_ob, '__path__'):
        for importer, modname, ispkg in walk_packages(
            package_ob.__path__, package_ob.__name__+'.'
            ):
            try:
                __import__(modname)
            except ImportError:
                pass
            else:
                to_search.append(sys.modules[modname])
    
    tables_for_source = set()
    for searchable in to_search:
        for name,ob in getmembers(searchable):
            table = getattr(ob, '__table__', None)
            if table is None:
                continue
            if ob.__module__.startswith(package):
                tables_for_source.add(table)

    for table in tables:
        tables_for_source.add(table)

    return Source(*tables_for_source)

class Config:
    """
    A configuration for a particular database to allow control
    of the schema of that database.

    :param sources: The :class:`Source` instances from which to create
      this configuration.
    """

    def __init__(self, *sources):
        self.tables = set()
        problem_tables = set()
        for source in sources:
            for table in source.metadata.tables.keys():
                if table in self.tables:
                    problem_tables.add(table)
                else:
                    self.tables.add(table)
        if problem_tables:
            raise ValueError('Tables present in more than one Source: %s' % (
                ', '.join(problem_tables)
                ))
        self.sources = sources
        # keep track of which tables *aren't managed by a particular source
        self.excludes = {}
        for source in sources:
            excludes = self.tables - set(source.metadata.tables.keys())
            self.excludes[source] = excludes
    
class Scripts:
    """
    A command-line harness for performing schema control functions on
    a database. You should instantiate this in a small python script and
    call it when the script is run as a command, eg::

      from mortar_rdb.controlled import Scripts
      from sample.model import config

      scripts = Scripts('sqlite://', config, True)

      if __name__=='__main__':
          script()

    Writing the script in this style also allows :obj:`scripts` to be
    used as a :mod:setuptools entry point.

    :param url:
      The :mod:`SQLAlchemy` url to connect to the database to be
      managed.

    :param config:
      A :class:`Config` instance describing the schema of the database
      to be managed.

    :param failsafe:
      A boolean value that should be ``True`` if it's okay for the
      database being managed to have all its tables dropped. For
      obvious reasons, this should be ``False`` when managing your
      production database.
    """

    def __init__(self,url,config,failsafe):
        # avoid import loop
        self.default_url = url
        self.config = config
        self.failsafe = failsafe

    def create(self):
        """
        Create all the tables in the configuration
        in the database
        """
        names = Inspector.from_engine(self.engine).get_table_names()
        if names:
            print()
            print("Refusing to create as the following tables exist:")
            for name in names:
                print(name)
            return
        print()
        print("Creating the following tables:")
        for source in self.config.sources:
            for table in source.metadata.sorted_tables:
                print(table.name)
            source.metadata.create_all(self.engine)

    def drop(self):
        "Drop all tables in the database"
        # avoid import loop
        from . import drop_tables
        if self.failsafe:
            print("Dropping all tables.")
            drop_tables(self.engine)
        else:
            print("Refusing to drop all tables due to failsafe.")

    def __call__(self, argv=None):
        parser = ArgumentParser(
            formatter_class=RawDescriptionHelpFormatter,
            description="""
The database to be acted on is at:
%s

They following tables are in the current configuration:
%s
""" % ( self.default_url,
        ', '.join(self.config.tables)))
        
        parser.add_argument(
            '--url',
            default = self.default_url,
            help='Override the database url used.'
            )

        commands = parser.add_subparsers()

        for name in dir(self.__class__):
            if name.startswith('_'):
                continue
            doc = getattr(self,name).__doc__.strip()
            command =  commands.add_parser(
                name,
                help=doc
                )
            command.set_defaults(method=getattr(self,name))

        options = parser.parse_args(argv)

        self.engine = create_engine(options.url)
        print("For database at %s:" % options.url)
        options.method()
        
        
        


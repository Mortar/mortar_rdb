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
used. :mod:`sqlalchemy-migrate` is used to do the bulk of the work.

Packages, Models and Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:mod:`SQLAlchemy` uses :class:`~sqlalchemy.schema.Table` objects that
are mapped to one or more model classes. These objects are defined
within python packages

:mod:`mortar_rdb.controlled` makes the assumption that each package that
defines any :class:`~sqlalchemy.schema.Table` will have one
:mod:`sqlalchemy-migrate` repository and this repository will be
responsible for looking after the versioning of all tables defined
within the package.

Configurations
~~~~~~~~~~~~~~

A single database may contain tables that are define in more that one
package. For example, an authentication package may contain some
table definitions for storing users and their permission. That package
may be used by an application which also contains a package that
defines its own tables.

A :class:`~mortar_rdb.controlled.Config` is a way of expressing
which tables should be expected and which :mod:`sqlalchemy-migrate`
repositories should contain controls for those tables for a particular
database.

It general, it is recommended that a
:class:`~mortar_rdb.controlled.Config` is defined once, in whatever
package 'owns' a particular database. For example, an application may
define a configuration for its own tables and those of any packages on
which it relies, such as the hypothetical authentication package
described above. If another application wants to use this
application's database, it can import the configuration and check that
the database structure matches that expected by the code it is
currently using.

.. warning::

  Controlled Schemas can't currently protect you if Application A
  upgrades a database while Application B is using it. The earliest it
  can help Application B is when it re-registers its sessions, likely
  at restart time, when it will hopefully refuse to start!

"""

from argparse import ArgumentParser,RawDescriptionHelpFormatter
from inspect import getmembers
from migrate.versioning.repository import Repository
from migrate.versioning.schema import ControlledSchema
from migrate.versioning.schemadiff import SchemaDiff
from os import remove, listdir
from os.path import join, isabs
from pkg_resources import resource_filename
from pkgutil import walk_packages
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.engine.reflection import Inspector
from types import ClassType
from zope.dottedname.resolve import resolve

import sys

def create_repository(path,id):
    # name isn't used
    r = Repository.create(path,id)
    # remove braindead manage script
    remove(join(path,'manage.py'))
    return r

default_repo_name = 'db_versioning'

def create_main():
    
    def repo(options,parser):
        id = options.id or options.package
        if id is None:
            parser.error('--id must be specified when --path is used')
        create_repository(options.path,id)
        print "Created repository with id %r at:\n%s" % (
            id,
            options.path,
            )

    def script(options,parser):
        repo = Repository(options.path)
        def list_scripts():
            return set(listdir(join(repo.path,'versions')))
        before = list_scripts()
        repo.create_script(options.description)
        after = list_scripts()
        print "Created script for version %i at:\n%s" % (
            repo.latest,
            '\n'.join([join(repo.path,'versions',n) for n in after-before])
            )
    
    parser = ArgumentParser()
    
    commands = parser.add_subparsers(
        title='Commands',
        help='Do %(prog)s {command} --help to '
             'get help for a particular command'
        )

    command =  commands.add_parser('repo',help="Create a repository")
    command.set_defaults(method=repo)
    command.add_argument(
        '--path',
        action='store',
        help='Path at which to create the repository.'
        )
    command.add_argument(
        '--package',
        action='store',
        help='Package in which to create the repository.'
        )
    command.add_argument(
        '--id',
        help='The id to give the new repository. If a package is '
             'specified, the full dotted name of the package is used '
             'by default. If --path is specified, then --id must be '
             'passed.'
        )
    command.add_argument(
        '--name',
        default=default_repo_name,
        help='The name of the repository within the package. '
             'Ignored if --path is used.',
        )

    command =  commands.add_parser('script',help="Create a migration script")
    command.set_defaults(method=script)
    command.add_argument(
        '--path',
        help='Path to the repository in which the '
             'script should be created.'
        )
    command.add_argument(
        '--package',
        help='Package in which to find the repository '
             'to create the script in.'
        )
    command.add_argument(
        '--name',
        default=default_repo_name,
        help='The name of the repository within the package. '
             'Ignored if --path is used.',
        )

    command.add_argument(
        'description',
        help='The description of the new migration step.'
         )
    
    options = parser.parse_args()

    if options.package:
        options.path = resource_filename(options.package,options.name)
        
    options.method(options,parser)

class Source:
    """
    A collection of tables and the repository that manages them.
    
    :param repository_path:
          A path where a :mod:`sqlalchemy-migrate` repository exists
          that manages the tables contained in this source.

    :param tables:
          A sequence of :class:`~sqlalchemy.schema.Table` objects that
          contain all the tables that will be managed by the
          repository in this Source. 
    """

    def __init__(self,repository_path,*tables):
        
        self.repository = Repository(repository_path)
        
        self.metadata = MetaData()
        
        for table in tables:
            if not isinstance(table,Table):
                raise TypeError(
                    '%r must be a Table object or a declaratively '
                    'mapped model class.' % (
                        table
                        ))
            table.tometadata(self.metadata)

def scan(package,tables=(),repository_path=default_repo_name):
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
          
    :param repository_path:
          A path relative to the package where a
          :mod:`sqlalchemy-migrate` repository will be found.
    """
    if not isabs(repository_path):
        repository_path = resource_filename(package,repository_path)

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
            table = getattr(ob,'__table__',None)
            if table is None:
                continue
            if ob.__module__.startswith(package):
                tables_for_source.add(table)

    for table in tables:
        tables_for_source.add(table)

    return Source(repository_path,*tables_for_source)

class Config:
    """
    A configuration for a particular database to allow control
    of the schema of that database.

    :param sources: The :class:`Source` instances from which to create
      this configuration.
    """

    def __init__(self,*sources):
        self.tables = set()
        problem_tables = set()
        self.repos = set()
        problem_repos = set()
        for source in sources:
            for table in source.metadata.tables.keys():
                if table in self.tables:
                    problem_tables.add(table)
                else:
                    self.tables.add(table)
            if source.repository.path in self.repos:
                problem_repos.add(source.repository.path)
            else:
                self.repos.add(source.repository.path)
        if problem_tables or problem_repos:
            for t,s in (('Tables',problem_tables),
                        ('Repositories',problem_repos)):
                if not s:
                    continue
                raise ValueError('%s present in more than one Source: %s' % (
                        t, ', '.join(s)
                        ))
        self.sources = sources
        self.excludes = {}
        for source in sources:
            excludes = self.tables - set(source.metadata.tables.keys())
            excludes.add(source.repository.version_table)
            self.excludes[source] = excludes

class ValidationException(Exception):
    """
    An exception raised when validation with :func:`~mortar_rdb.validate`
    fails. In addition to a string representation that describes the
    problems found, it also has several useful attributes:

    .. attribute:: repos
    
      A sorted list of paths to the repositories on disk that caused
      this exception to be raised.
      
    .. attribute:: version_mismatches
    
      A dictionary mapping repository path to a tuple of version found
      and version expected in the form ``(actual,excected)``.

    .. attribute:: diffs
    
      A dictionary mapping repository path to a textual representation
      of the differences between the schema found in the database and
      the one expressed in the model.

    .. attribute:: body
    
        This is a textual representation of all the problems that
        caused validation to fail, suitable for printing.
    """
    def __init__(self):
        self._repos=set()
        self.version_mismatches = {}
        self.diffs = {}
    
    def __str__(self):
        return '<ValidationException>%s</ValidationException>' % self.body
    
    __repr__ = __str__

    def __nonzero__(self):
        return bool(self._repos)

    @property
    def body(self):
        result = []
        for repo in self.repos:
            result.extend(('','Repository at:',repo))
            if repo in self.version_mismatches:
                result.append('Version was %i, should be %i' % (
                        self.version_mismatches[repo]
                        ))
            if repo in self.diffs:
                result.append(self.diffs[repo])
        if result:
            return '\n'.join(result)+'\n'

    @property
    def repos(self):
        return sorted(self._repos)

    def version_mismatch(self,repo,expected,actual):
        self._repos.add(repo)
        self.version_mismatches[repo]=(actual,expected)

    def diff(self,repo,diff):
        self._repos.add(repo)
        self.diffs[repo]=diff.rstrip()
    
def validate(engine,config,versions=True):
    """
    This will validate whether the database schema expressed in the
    :class:`Config` matches that found in the
    database connected to by the
    :class:`~sqlalchemy.engine.base.Engine`.

    If `versions` is `True`, the validation will fail if the latest
    version found in the repository of each of the :class:`Source`s in
    the :class:`Config` does not match that found for that repository
    in the database.
    """
    e = ValidationException()
    for source in config.sources:
        if versions:
            schema = ControlledSchema(
                engine,
                source.repository
                )
            d_version = schema.version
            r_version = source.repository.latest
            if d_version!=r_version:
                e.version_mismatch(source.repository.path,
                                   int(r_version),
                                   int(d_version))
        diff = SchemaDiff(
            metadataA=source.metadata,
            metadataB=MetaData(engine, reflect=True),
            labelA='repository',
            labelB='database',
            excludeTables=config.excludes[source],
            )
        if diff:
            e.diff(source.repository.path,str(diff))
    if e:
        raise e
    
class Scripts:
    """
    A command-line harness for performing schema control functions on
    a database. You should instantiate this in a small python script and
    call it when the script is run as a command, eg::

      from mortar_rdb.controlled import Scripts
      from sample.model import config

      scripts = Scripts('sqlite://',config,True)

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
        from . import create_engine
        self.engine = create_engine(url)
        self.url = url
        self.config = config
        self.failsafe = failsafe


    def _sources(self,schema=True):
        for source in self.config.sources:
            print
            print "Repository at:"
            print source.repository.path
            if schema:
                yield (
                    source,
                    ControlledSchema(
                        self.engine,
                        source.repository
                        )
                    )
            else:
                yield source
        
    def create(self):
        """
        Create all the tables in the configuration
        in the database
        """
        names = Inspector.from_engine(self.engine).get_table_names()
        if names:
            print
            print "Refusing to create as the following tables exist:"
            for name in names:
                print name
            return
        for source in self._sources(schema=False):
            print "Creating the following tables:"
            for table in source.metadata.sorted_tables:
                print table.name
            print "Setting database version to:"
            print source.repository.latest
            source.metadata.create_all(self.engine)
            ControlledSchema.create(
                self.engine,
                source.repository,
                source.repository.latest
                )

    def drop(self):
        "Drop all tables in the database"
        # avoid import loop
        from . import drop_tables
        if self.failsafe:
            print "Dropping all tables."
            drop_tables(self.engine)
        else:
            print "Refusing to drop all tables due to failsafe."

    def _validate(self):
        try:
            validate(self.engine,self.config,versions=False)
        except ValidationException,e:
            print e.body
            return False
        else:
            return True
    
    def control(self):
        "Start controlling the database"
        if not self._validate():
            print
            print ("Cannot introduce controls as "
                   "model does not match database.")
            return
        for source in self._sources(schema=False):
            print "Setting database version to:"
            print source.repository.latest
            ControlledSchema.create(
                self.engine,
                source.repository,
                source.repository.latest
                )
    
    def check(self):
        "Check the structure and versions of the database"
        phail = False
        for source,schema in self._sources():
            # check version
            d_version = schema.version
            r_version = source.repository.latest
            if d_version == r_version:
                print "Version is correctly at %i." % r_version
            else:
                print "Version was %i, should be %i." % (
                    d_version,r_version
                    )
                phail = True
        if phail:
            return
        # check model matches!
        if self._validate():
            print "All tables are correct."
                
    def upgrade(self):
        "Upgrade the database"
        for source,schema in self._sources():
            changeset = schema.changeset()
            if len(changeset):
                for ver, change in changeset:
                    nextver = ver + changeset.step
                    print '%s -> %s (%s)'% ( ver, nextver, change)
                    schema.runchange(ver, change, changeset.step)
                    print 'done'
            else:
                print "No upgrade necessary, version at %i." % schema.version

        # check model matches!
        self._validate()

    def __call__(self):
        parser = ArgumentParser(
            formatter_class=RawDescriptionHelpFormatter,
            description="""
The database to be acted on is at:
%s

The following repositories are in the configuration:
%s

Between them, they control the following tables:
%s
""" % (
                self.url,
                '\n'.join(self.config.repos),
                ', '.join(self.config.tables),
            )
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

        options = parser.parse_args()
        print "For database at %s:" % self.url
        options.method()
        
        
        


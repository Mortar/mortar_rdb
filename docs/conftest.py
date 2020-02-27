import re
import sys
import textwrap
from doctest import REPORT_NDIFF, ELLIPSIS
from functools import partial
from io import StringIO
from os.path import join
from unittest import TextTestRunner, makeSuite

from sybil import Sybil, Region
from sybil.parsers.doctest import DocTestParser
from sybil.parsers.codeblock import CodeBlockParser
from sybil.parsers.capture import parse_captures
from testfixtures import compare, TempDirectory, Replacer, OutputCapture

from testfixtures.sybil import FileParser

from mortar_rdb.testing import TestingBase

BASHBLOCK_START = re.compile('^.+\$ ',re.MULTILINE)
BASHBLOCK_END = re.compile(r'(\n\Z|\n(?=\S))')


def parse_bash_blocks(document):
    if document.path.endswith('development.txt'):
        return
    for start_match, end_match, source in document.find_region_sources(
        BASHBLOCK_START, BASHBLOCK_END
    ):
        command, output = source.split('\n', 1)
        parsed = command, textwrap.dedent(output)
        yield Region(start_match.start(), end_match.end(),
                     parsed, evaluate_bash_block)


def evaluate_bash_block(example):
    command, output = example.parsed

    def sample_script(dotted, func):
        sample = __import__('sample.' + dotted)
        obj = sample
        for name in dotted.split('.'):
            obj = getattr(obj,name)
        getattr(obj,func)()

    def do_nothing(*args):
        pass

    commands = {
        'bin/db': (partial(sample_script, 'db', 'scripts'), False),
        'bin/run': (partial(sample_script, 'run', 'main'), False),
        'DB_URL=mysql://scott:tiger@localhost/test': (do_nothing, False),
        }

    command, args = command.split(None, 1)
    func, needs_ver = commands[command]

    with Replacer() as r:
        r.replace('sys.argv',[command]+args.split())
        try:
            with OutputCapture() as o:
                func()
        except SystemExit as e:  # pragma: no cover
            print("Output:")
            print(o.output.getvalue())
            raise
        except Exception as e:  # pragma: no cover
            actual = '...traceback...\n'+repr(e)
        else:
            actual = o.output.getvalue()
    actual = actual.replace(example.namespace['dir'].path, '').strip()
    compare(actual, expected=output.rstrip())


def run_tests(case, run):
    output = StringIO()
    runner = TextTestRunner(output)
    result = runner.run(makeSuite(case))
    if result.errors or result.failures:  # pragma: no cover
        raise AssertionError('\n'+output.getvalue())
    compare(run, actual=result.testsRun)


def setup(namespace):
    namespace['tb'] = TestingBase()
    namespace['run_tests'] = run_tests
    namespace['dir'] = dir = TempDirectory()
    sys.path.append(dir.path)
    namespace['db_url']=db_url= 'sqlite:///' + join(dir.path, 'test.db')
    # make sample package
    dir.write('sample/__init__.py', b'')
    dir.write('sample/config.py', ("""
db_url = %r
is_production = False
""" % db_url).encode('ascii'))


def teardown(namespace):
    dir = namespace['dir']
    sys.path.remove(dir.path)
    dir.cleanup_all()
    namespace['tb'].restore()


pytest_collect_file = Sybil(
    parsers=[
        DocTestParser(optionflags=REPORT_NDIFF|ELLIPSIS),
        parse_captures,
        CodeBlockParser(['print_function']),
        FileParser('dir'),
        parse_bash_blocks,
    ],
    pattern='*.txt',
    setup=setup,
    teardown=teardown,
).pytest()

# Copyright (c) 2011-2013 Simplistix Ltd, 2015 Chris Withers
# See license.txt for license details.
from __future__ import print_function

from ..compat import StringIO
from doctest import REPORT_NDIFF,ELLIPSIS
from functools import partial
from mortar_rdb.testing import TestingBase
from manuel import Manuel, doctest, capture, codeblock
from manuel.testing import TestSuite
from os.path import dirname, join, pardir, splitext
from shutil import copytree
from testfixtures import TempDirectory, Replacer, OutputCapture, compare
from testfixtures.manuel import Files
from unittest import makeSuite, TextTestRunner

import re,sys
import textwrap

EXECUTEBLOCK_START = re.compile('^.+\$(.+)$',re.MULTILINE)
EXECUTEBLOCK_END = re.compile(r'(\n\Z|\n(?=\S))')

class ExecuteBlock(object):
    def __init__(self,command,output):
        self.command = command
        self.output = output

class Execute(Manuel):
    def __init__(self):
        Manuel.__init__(self,parsers=[self.parse],
                        evaluaters=[self.evaluate])
    def parse(self,document):
        for region in document.find_regions(EXECUTEBLOCK_START,
                                            EXECUTEBLOCK_END):
            region.parsed = ExecuteBlock(
                region.start_match.group(1),
                textwrap.dedent(region.source.split('\n',1)[1]).strip()
                )
            document.claim_region(region)

    def evaluate(self,region,document,globs):
        if not isinstance(region.parsed, ExecuteBlock):
            return
        block = region.parsed
        
        def sample_script(dotted, func):
            sample = __import__('sample.' + dotted)
            obj = sample
            for name in dotted.split('.'):
                obj = getattr(obj,name)
            getattr(obj,func)()

        def do_nothing(*args):
            pass
    
        commands = {
            'bin/db':(partial(sample_script, 'db', 'scripts'), False),
            'bin/run':(partial(sample_script, 'run', 'main'), False),
            'DB_URL=mysql://scott:tiger@localhost/test':(do_nothing, False),
            }

        command, args = block.command.split(None, 1)
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
        expected = block.output
        actual = actual.replace(globs['dir'].path,'')
        compare(expected, actual.strip())

def run_tests(case, run):
    output = StringIO()
    runner = TextTestRunner(output)
    result = runner.run(makeSuite(case))
    if result.errors or result.failures:  # pragma: no cover
        raise AssertionError('\n'+output.getvalue())
    compare(run, result.testsRun)

def setUp(test):
    test.globs['tb'] = TestingBase()
    test.globs['run_tests'] = run_tests
    test.globs['dir'] = dir = TempDirectory()
    sys.path.append(dir.path)
    test.globs['db_url']=db_url='sqlite:///'+join(dir.path,'test.db')
    # make sample package
    dir.write('sample/__init__.py', b'')
    dir.write('sample/config.py', ("""
db_url = %r
is_production = False
""" % db_url).encode('ascii'))
    
def tearDown(test):
    dir = test.globs['dir']
    sys.path.remove(dir.path)
    dir.cleanup_all()
    test.globs['tb'].restore()
    
def test_suite():
    m =  doctest.Manuel(optionflags=REPORT_NDIFF|ELLIPSIS)
    m += capture.Manuel()
    m += codeblock.Manuel()
    m += Files('dir')
    m += Execute()
    return TestSuite(
        m,
        join(dirname(__file__),pardir,pardir,'docs','use.txt'),
        join(dirname(__file__),pardir,pardir,'docs','sequences.txt'),
        setUp = setUp,
        tearDown = tearDown
        )

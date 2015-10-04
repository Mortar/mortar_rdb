# Copyright (c) 2011-2013 Simplistix Ltd
# See license.txt for license details.

from cStringIO import StringIO
from doctest import REPORT_NDIFF,ELLIPSIS
from functools import partial
from glob import glob
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
        if func is None:
            return
        if needs_ver:
            ver = 'sample'+str(globs['ver'])
            args = args.replace('sample', ver)

        with Replacer() as r:
            r.replace('sys.argv',[command]+args.split())
            try:
                with OutputCapture() as o:
                    func()
            except SystemExit,e:
                print "Output:"
                print o.output.getvalue()
                raise
            except Exception,e:
                actual = '...traceback...\n'+repr(e)
            else:
                actual = o.output.getvalue()
        expected = block.output
        actual = actual.replace(globs['dir'].path,'')
        if needs_ver:
            actual = actual.replace(ver,'sample')
        compare(expected, actual.strip())

def run_tests(case, run):
    output = StringIO()
    runner = TextTestRunner(output)
    result = runner.run(makeSuite(case))
    if result.errors or result.failures:
        raise AssertionError('\n'+output.getvalue())
    compare(run, result.testsRun)

def create_version(globs,ver):
    # a lot of work, but we need to do it
    # as reload simply doesn't fly :-/
    import mortar_rdb
    mortar_rdb._bases = {}
    dir = globs['dir']
    s = 'sample'+str(ver)
    copytree(dir.getpath('sample'),dir.getpath(s))
    for path in dir.actual(s,recursive=True):
        base,ext = splitext(path)
        if ext in ('.pyc','.pyo','.cfg') or not ext:
            continue
        fullpath = s+'/'+path
        dir.write(fullpath,dir.read(fullpath).replace('sample',s))
    globs['ver']=ver
    
def setUp(test):
    test.globs['tb'] = TestingBase()
    test.globs['run_tests'] = run_tests
    test.globs['create_version'] = partial(create_version,test.globs)
    test.globs['dir'] = dir = TempDirectory()
    sys.path.append(dir.path)
    test.globs['db_url']=db_url='sqlite:///'+join(dir.path,'test.db')
    # make sample package
    dir.write('sample/__init__.py','')
    dir.write('sample/config.py',"""
db_url = %r
is_production = False
""" % db_url)
    
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

# compatibility module for different python versions
import sys

if sys.version_info[:2] > (3, 0):
    from io import StringIO
    empty_str = "''"
    PY2 = False
else:
    from cStringIO import StringIO
    empty_str = "u''"
    PY2 = True

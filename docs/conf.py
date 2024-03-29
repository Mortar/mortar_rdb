# -*- coding: utf-8 -*-
import os, pkginfo, datetime

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
pkg_info = pkginfo.Develop(os.path.join(os.path.dirname(__file__),'..'))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'repoze.sphinx.autointerface',
    ]

intersphinx_mapping = dict(
    python=('http://docs.python.org/dev',None),
    sqlalchemy=('http://www.sqlalchemy.org/docs/',None)
    )

# General
source_suffix = '.txt'
master_doc = 'index'
project = pkg_info.name
current_year = datetime.datetime.now().year
copyright = '2011-2015 Simplistix Ltd, 2015-%s Chris Withers' % current_year
version = release = pkg_info.version
exclude_trees = ['_build']
unused_docs = ['description']
pygments_style = 'sphinx'

# Options for HTML output
if on_rtd:
    html_theme = 'default'
else:
    html_theme = 'classic'
htmlhelp_basename = project+'doc'

# Options for LaTeX output
latex_documents = [
  ('index',project+'.tex', project+u' Documentation',
   'Simplistix Ltd', 'manual'),
]

exclude_patterns = ['description.txt']


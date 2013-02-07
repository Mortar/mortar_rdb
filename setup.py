# Copyright (c) 2012 Simplistix Ltd
# See license.txt for license details.

import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)

setup(
    name='mortar_rdb',
    author='Chris Withers',
    version='2.0a',
    author_email='chris@simplistix.co.uk',
    license='MIT',
    description=(
        "SQLAlchemy and the component architecture tied together "
        "for easy use in multi-package projects for any framework"
        ),
    long_description=open(os.path.join(base_dir,'docs','description.txt')).read(),
    url='http://www.simplistix.co.uk/software/python/mortar_rdb',
    classifiers=[
    'Development Status :: 5 - Production/Stable',
    ],    
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires = (
        'argparse',
        'SQLAlchemy',
        'zope.component',
        'zope.dottedname',
        'zope.interface',
        'zope.sqlalchemy',
        ),
    extras_require=dict(
        test=[
            'mock',
            'manuel',
            'testfixtures >= 1.9.0',
            ],
        ),
    entry_points = {
        'console_scripts': [
            'mortar_rdb_create = mortar_rdb.controlled:create_main',
            ],
        },
    )


import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)
description_path = os.path.join(base_dir, 'docs', 'description.txt')

setup(
    name='mortar_rdb',
    author='Chris Withers',
    version='3.0.0',
    author_email='chris@simplistix.co.uk',
    license='MIT',
    description=(
        "SQLAlchemy and the component architecture tied together "
        "for easy use in multi-package projects for any framework"
        ),
    long_description=open(description_path).read(),
    url='https://github.com/Mortar/mortar_rdb',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    python_requires='>=3.6',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires = (
        'SQLAlchemy<1.4',
        'zope.component<5',
        'zope.dottedname',
        'zope.interface',
        'zope.sqlalchemy<1.2',
        ),
    extras_require=dict(
        test=[
            'pytest',
            'pytest-cov',
            'mock',
            'sybil>=3',
            'testfixtures<8',
            ],
        build=[
            'sphinx',
            'repoze.sphinx.autointerface',
            'pkginfo',
            'setuptools-git',
            'twine',
            'wheel',
        ]
        ),
    )


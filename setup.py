from setuptools import setup,find_packages

setup(
    name='glc.db',
    install_requires = (
        'argparse',
        'SQLAlchemy >= 0.6.4',
        'sqlalchemy-migrate',
        'zope.component',
        'zope.interface',
        'zope.sqlalchemy',
        # undeclared sqlalchemy-migrate dependency
        'setuptools',
        # testing dependencies
        'glc.testing',
        'mock',
        'manuel',
        'testfixtures',
        ),
    entry_points = {
        'console_scripts': [
            'glc_db_create = glc.db.controlled:create_main',
            ],
        },
    # boilerplate below here
    version='1.0dev',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    )


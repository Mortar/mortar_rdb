from setuptools import setup,find_packages

setup(
    name='mortar_rdb',
    author='Chris Withers',
    version='1.0dev',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires = (
        'argparse',
        'SQLAlchemy >= 0.6.4',
        'sqlalchemy-migrate > 0.6',
        'zope.component',
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


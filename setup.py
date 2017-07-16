from setuptools import setup, find_packages

setup(
    name='sqlify',
    version='1.0.0b3',
    description='A library of tools for converting CSV, TXT, and HTML formats to SQL',
    long_description='A library of tools for converting CSV and TXT formats to SQL. Advanced features include an HTML <table> parser and SQLite to PostgreSQL conversion.',
    url='https://github.com/vincentlaucsb/sqlify',
    author='Vincent La',
    author_email='vincela9@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    keywords='sql convert txt csv text delimited',
    packages=find_packages(exclude=['docs', 'scratch', 'setup', 'tests*']),
    install_requires=[
        'psycopg2'
    ]
)
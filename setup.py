from setuptools import setup, find_packages

setup(
    name='sqlify',
    version='1.0.0a1',
    description='A library of tools for converting CSV and TXT formats to SQL',
    long_description='A library of tools for converting CSV and TXT formats to SQL',
    author='Vincent La',
    author_email='vincela9@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    keywords='sql convert txt csv text delimited',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'Click',
        'psycopg2'
    ],
    entry_points='''
        [console_scripts]
        tosql=sqlify.cli.tosql:cli
        sqlify=sqlify.cli.sqlify_quick:cli
    '''
)
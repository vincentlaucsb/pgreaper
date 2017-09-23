from setuptools import setup, find_packages
from setuptools.extension import Extension

USE_CYTHON = True

try:
    from Cython.Distutils import build_ext
    from Cython.Build import cythonize
except ImportError:
    USE_CYTHON = False

def cython_or_c(ext):
    if USE_CYTHON:
        return cythonize(ext)
    else:
        for e in ext:        
            for i, j in enumerate(e.sources):
                e.sources[i] = j.replace('.pyx', '.c')
        return ext
    
extensions = cython_or_c([
    Extension(
        "pgreaper.core.table",
        sources=["pgreaper/core/table.pyx"],
    ),
    Extension(
        "pgreaper.io.json_tools",
        sources=["pgreaper/io/json_tools.pyx"],
        language="c++",
    )
])

# Enable creating Sphinx documentation
for ext in extensions:
    ext.cython_directives = {
        'embedsignature': True,
        'binding': True,
        'linetrace': True}

setup(
    name='pgreaper',
    cmdclass={'build_ext': build_ext},
    version='1.0.0',
    description='A library of tools for converting CSV, TXT, and HTML formats to SQL',
    long_description='A library of tools for converting CSV and TXT formats to SQL. Advanced features include an HTML <table> parser and SQLite to PostgreSQL conversion.',
    url='https://github.com/vincentlaucsb/pgreaper',
    author='Vincent La',
    author_email='vincela9@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    keywords='sql convert txt csv text delimited',
    packages=find_packages(exclude=['benchmarks', 'dev', 'docs', 'scratch', 'setup', 'tests*']),
    install_requires=[
        'psycopg2',
        'Click',
        'csvmorph',
    ],
    entry_points='''
        [console_scripts]
        pgreaper=pgreaper.cli:cli_copy
    ''',
    ext_modules = cythonize(extensions),
    include_package_data=True
)
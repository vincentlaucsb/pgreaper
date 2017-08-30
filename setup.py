from setuptools import setup, find_packages
from setuptools.extension import Extension

USE_CYTHON = True
if USE_CYTHON:
    from Cython.Build import cythonize

# def cython_or_c(ext, *args, **kwargs):
    # if not USE_CYTHON:
        # for i in ext:
            # i.sources = [j.replace('.pyx', '.c') for j in i.sources]
            
        # return ext
    # else:
        # return cythonize(ext, *args, **kwargs)
    
# extensions = cython_or_c([
    # Extension(
        # "pgreaper.core.from_text",
        # sources=["pgreaper/core/from_text.pyx"],
    # ),
    # Extension(
        # "pgreaper.core.table",
        # sources=["pgreaper/core/table.pyx"],
    # )
# ])

extensions = [
    Extension(
        "pgreaper.core.from_text",
        sources=["pgreaper/core/from_text.pyx"],
    ),
    Extension(
        "pgreaper.core.table",
        sources=["pgreaper/core/table.pyx"],
    )
]

setup(
    name='pgreaper',
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
    ext_modules = cythonize(extensions, gdb_debug=True),
    install_requires=[
        'psycopg2'
    ],
    include_package_data=True
)
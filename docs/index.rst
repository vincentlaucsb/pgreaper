.. sqlify documentation master file, created by
   sphinx-quickstart on Sun Apr 30 21:27:00 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SQLify 1.0.0a1 Documentation
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

What is SQLify?
----------------
SQLify is a tool written in Python that allows for easy conversion of common data sources, like text files and comma-separated values (CSV) into SQL databases. Currently, SQLite and PostgreSQL and supported.

https://github.com/vincentlaucsb/sqlify

SQLite
-------
SQLify loads files into SQLite by lazy loading them with the parsers available
in Python's standard library and bulk insert statements. With later versions 
of `sqlite3` (a built-in Python module), 40MB CSV files can be loaded instantaneously and 2GB text files take less than three minutes. Because files are lazy-loaded, SQLify will not crash due to insufficient memory.

PostgreSQL
-----------
In regards to Postgres, SQLify acts as a wrapper around Postgres' fast COPY command. It does much of the tedious work for you, including creating tables
with the correct column names and types.

Other Features
---------------
SQLify tries to make loading files into SQL as painless as possible, so you
work on what really matters. Other features include:
 * Automatically correcting problematic column names

Documentation Contents
=======================

.. toctree::
   
   python

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

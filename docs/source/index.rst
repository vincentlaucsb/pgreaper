.. pgreaper documentation master file, created by
   sphinx-quickstart on Sun Apr 30 21:27:00 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pgreaper 1.0.0 Documentation
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

PostgreSQL Default Settings
----------------------------
Before uploading to Postgres, you may want to configure the default 
connection settings. If default settings are provided, PGReaper can use 
these to create new databases so you won't have to create them manually.

.. autofunction:: pgreaper.settings
   
Uploading TXTs and CSVs to Postgres
-------------------------------------

.. toctree::

   csv
   
Reading JSON
-------------

.. toctree::
   
   json

Reading From Compressed (ZIP) Files
------------------------------------
PGReaper is capable of copying specific files located in ZIP archives without decompressing them. (If you are interested in reading in GZIP, BZIP,
or LZMA compressed files, use the `compression` parameter on the 
`copy_csv()` function.)

.. toctree::

   zip

The `Table` Data Structure
---------------------------
PGReaper contains a two-dimensional data structure creatively named `Table`.
These are lightweight structures which are built on Python's `list` containers
but contain a lot of features for easily mapping them into SQL tables.

.. toctree::
   
   table
   
pandas Integration
----------------------------
.. toctree::

   pandas
   pandas_example

HTML Parsing
--------------
pgreaper contains a rich HTML parsing module featuring automated `<table>` parsing and Jupyter notebook integration. Because it is a large module on its own, it 
has its own documentation page.

.. toctree::

   html_table
   
Internals
----------
Information for maintainers and forkers of `pgreaper`.\

.. toctree::
   
   internals
   internals_mappings
   
Index
------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
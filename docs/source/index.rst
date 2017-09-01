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

General Details
----------------
For general information about how PGReaper does what it does, such as schema inference, you should read this_.

.. _this: postgres.html

.. toctree:: postgres
   :maxdepth: 2
   
Uploading CSVs to Postgres
---------------------------
.. autofunction:: pgreaper.copy_csv

Reading From Compressed (ZIP) Files
------------------------------------
PGReaper is capable of copying specific files located in ZIP archives without decompressing them.

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

HTML Parsing
--------------
pgreaper contains a rich HTML parsing module featuring automated `<table>` parsing and Jupyter notebook integration. Because it is a large module on its own, it 
has its own documentation page.

.. toctree::

   html_table
   
Extra Features
---------------
PGReaper also has features which, while aren't directly related to PostgreSQL,
frequently come up in data analysis.

.. toctree::
   :maxdepth: 3
   
   text_to_text

Internals
----------
Information for maintainers and forkers of `pgreaper`.
   
Index
------
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
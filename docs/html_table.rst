HTML Table Parsing
====================================================================

Introduction
-------------
A lot of useful data is stored in HTML tables, but parsing HTML is an arduous task. Using Python's standard library `html.parser` module, SQLify tries to simplify the task of parsing HTML tables by:
 * Creating a list of separate HTML tables
 * Trying to automatically find column headers
 * Handling different table designs, i.e. handling
    * <tbody> and <thead> tags
    * rowspan and colspan attributes
   
.. note:: Using SQLify's HTML parser in conjunction with Jupyter notebooks is recommended.
   
Step 1: Reading in HTML
------------------------
There are two avenues for reading in HTML.

a) Locally Saved HTML Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: sqlify.html.from_file
   
b) From the Web
~~~~~~~~~~~~~~~~~~
.. autofunction:: sqlify.html.from_url
   
Step 2: Reviewing the Output
-----------------------------
The functions above return `TableBrowser` objects, which are basically lists of HTML tables that were found. If viewing in Jupyter Notebook, the code above will display every table with an index next to the name of the table, e.g. `[5] Players of the week`.

.. autoclass:: sqlify.html.parser.TableBrowser

Step 3: Cleaning the Tables
----------------------------
If the tables you wanted were parsed 100% correctly and don't require any further processing steps, proceed to step 4. Otherwise, read on.

As seen above, when using the indexing operator on a `TableBrowser` object, you will get a 
`Table` object back. `Table` objects support a small set of data cleaning methods and contain
attributes you may want to use or modify.

.. autoclass:: sqlify.core.Table
   :members: delete, apply, aggregate, mutate, reorder
   :private-members: 

Step 4: Saving the Results
-------------------------------
After you've cleaned the Table to your satisfaction, you can save the results as either a:
 * CSV file
 * JSON file
 * SQLite Table
 * PostgreSQL Table

.. autofunction:: sqlify.table_to_csv
.. autofunction:: sqlify.table_to_json
.. autofunction:: sqlify.table_to_sqlite  
.. autofunction:: sqlify.table_to_pg
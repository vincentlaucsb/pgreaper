PGReaper Internals: Schema Inference
=====================================

This page documents the interals of PGReaper. Unless you are developing, maintaining, forking, or just really curious in PGReaper, this is not really going to be of interest to you.

.. automodule:: pgreaper.core.table

ColumnList
"""""""""""
Originally, the column names and types for a `Table` were stored as the `col_names` and `col_types` attributes respectively.
Simply being lists of strings, this approach--while simple--had many limitations.
As PGReaper expanded its capabilities, more and more were being demanded of these lists, such as:

 * Making sure `col_names` and `col_types` were of the same length
 * Returning SQL safe column names while somehow being to keep track of the original column names
 * Validating primary keys, i.e. making sure they referred to columns that actually existed
 * Comparing column lists to other column lists to see if one was a subset of another or not
    * This comes up when performing UPSERTs against existing SQL tables
 * Comparing column lists to other column lists to see if they were really the same columns in different orders (a common problem with JSON parsing)
 * Having a method to return the integer index corresponding to a column name
 * Taking a list of column names and mapping to to their respective integer indices (again, comes up in JSON parsing and also used to implement the `add_dict()` method
 
 Adding all this code to the `Table` structure made it messy, confusing, and harder to test. Therefore, a standalone class that managed column information was created.

.. autoclass:: pgreaper.core.column_list.ColumnList
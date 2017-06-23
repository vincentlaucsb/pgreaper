SQLite Reference
=====================

To access the commands below, include the following in your script:

.. code-block:: python
   
   import sqlify
   
Converting Text Files (TXT)
----------------------------
.. code-block:: python

   sqlify.text_to_sqlite(file, database, name, delimiter, header=0, na_values=None, skip_lines=None)

Essential Arguments
~~~~~~~~~~~~~~~~~~~~
   
* file: The name of the file to be converted
* database: The name of the database the results should be stored in
* name: The name of the resultant table in the SQL database (default: Use filename but without extension (e.g. 'file.txt' -> 'file')
* delimiter: How the file is separated

  - Default: ' ' for .txt files and ',' for .csv files
  
* header: The row number of the header

  - Default: 0 (i.e. the first row of the file is treated as a header)
  - Should be set to **None** if there is no header
 
* skip_lines: How many lines to skip

  - Default: None
  - Note: If header is specified, the header row and all lines preceding it are ignored

Other Arguments
~~~~~~~~~~~~~~~~

* na_values: How null values are encoded (default: None)

  - SQLify will automatically convert any values that are exactly like **na_values** into NULL
  - If missing values are a significant part of your data set, this will result in a smaller database
  - However, it may significantly slow down the conversion process (especially once you get into the gigabyte range)

Converting Comma-Separated Values (CSV)
----------------------------------------

- This function can also be used to convert other delimiter-seperated values (DSV) as long as the correct delimiter is specified
 
- For example, TSV (tab-separted values)
  
.. code-block:: python
    
   sqlify.csv_to_sqlite(...)
   
- Arguments are the same as for converting text files
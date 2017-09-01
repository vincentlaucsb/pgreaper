PostgreSQL Uploading Details
=============================

Loading Large Files
--------------------
All files are loaded in chunks of several thousand rows at a time to avoid running out of memory.
Actual memory usage depends on various factors, like how many columns a dataset has, and how 
effectively Python can garbage collect unused chunks.

Schema Inference
-----------------
File Uploads
""""""""""""""
When loading from a file, columns types are inferred based on the data type
of the entries in the first 7500 lines of a file. Currently, PGReaper is able 
to differentiate between text, integer, and floating point values in files.
Ideally, we would want to scan the entire file to get 100% accurate data types,
but this would take unacceptably long for large files--especially those larger
than a gigabyte.

As a result, it is possible that after the first 7500 rows, a field may contain
a value that is incompatible with the established schema. These rows are stored 
in a separate table with the naming scheme `<original table name>_reject`.

Table Uploads
""""""""""""""
When uploading `Table` or `pandas.DataFrame` structures from Python, PGReaper
will keep a count of every single data type inserted into a column by using the
`type()` function. In the case of mixed types, PGReaper will choose the column type
that allows both values to be stored in the same column.

Caveat: Numeric Strings
~~~~~~~~~~~~~~~~~~~~~~~~
Unlike when uploading from files, numeric strings in `Table` or `DataFrame` objects
count as strings, not as integers or floats. Therefore, you should typecast
strings to their appropriate type.
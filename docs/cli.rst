Command Line Interface
=====================

The command line interface is self-explanatory within a few minutes of using it.

Features
---------
* The interactive interface allows you to quickly identify the main features of data sources, e.g.:
 * How they are delimited
 * Whether or not they have a header row
 * The names of the columns as well as their composition

Limitations
------------

* The command line interface allows you to access most of SQLify's main features without ever writing a single line of code
* However, because it the CLI is focused on simplicity and ease-of-use, not all features are available
* SQLify's pre-processing features are only available through the Python API
 * For example, this includes applying transformations to data (aside from converting missing values to NULL)
# sqlify
A Python library for converting data from various formats into SQL tables.

## Basic Usage
First, import the library:
`
  import sqlify
`

## Converting Files to SQL databases
These arguments are used by all functions:
* file: The name of the file
* name: See below
* database: The name of the sqlite3 database to save to
* delimiter: How the values in the data are separated
* header: If set to `True`, treats the first row of data as a list of column names (default: True)
* col_names: If `header=False`, this is used to manually specify the table's column names
* col_types: The data types of the columns (see below for more details)
* p_key: The index of the column to be used as a PRIMARY KEY (default: None)

### `name` argument
The `name` argument used in one of two ways
1. To indicate the data should be stored in a single SQL table. To do this, `name` should simply be a string containing the intended name of the SQL table.
2. To indicate the data (or a subset of it) should be stored in multiple SQL tables.
   1. To do this, `name` should be a dictionary of SQL table names to values specifying which subset of the data should that specific table should contain.
   1. The values of this dictionary can be:
       1. Integers: Representing the indices of columns
       1. Tuples: Representing a range of columns, e.g. `(0, 3)` means the corresponding table should contain columns 0, 1, 2, and 3.
       1. Strings: The name of a column (only works if header=True is set)
       1. Lists: A combination of any of the above options
Example
```
name = {
  'sex_age': (3, 152),
  'race': (153, 212),
  'hispanic': (213, 226),
  'hispanic_and_race': (227, 260),
  'relationship': (261, 300),
  'households': (301, 338),
  'housing_occupancy': (339, 360),
  'housing_tenure': (361, 374)
}
```

### `col_types` argument
The `col_types` argument should either be a string or a list of the data types of the table's columns.
* If it is a string, all of the columns' data types will be set to that string.
* If it is a list, it should be as long as the number of columns in the table.

If it is not specified, sqlify will guess the data types of the columns by analyzing the data contained.

### Functions
`sqlify.text_to_sql(file, database, name, header, p_key, delimiter):`

`sqlify.csv_to_sql(file, database, name, header, p_key, delimiter=','):`
* skip_lines: Ignores the first `n` lines of data (default: 0)
    * Works independently of the `header` argument
    
## Converting Files to Python Objects
Instead of directly converting files to SQL databases, you also have the option of converting them to `Table` objects. This is useful if you intend on cleaning the data more before storing them into an SQL database.

### What are Tables?
Tables are a thin wrapper over vanilla Python lists, and are similar to R data frames. Whenever a data file--like a CSV or text file--is read, sqlify converts the contents into a Table object. A Table object is essentially a list of lists, where every list within the list represents a row of data.

### Table Attributes
Attribute | Description
----------|------------
`tbl.name` | The name of the corresponding SQL table
`tbl.col_names` | A list of the names of the table's columns. The n-th name corresponds to the n-th column.
`tbl.col_types` | A list of the data types of the table's columns

### Table Methods
`tbl[n]`
* Returns the n-th row of data

`tbl[COLUMN NAME]`
* Retrieves a specific column of data
* `COLUMN NAME` should be contained inside `tbl.col_names`

`tbl[COLUMN NAME].apply(func)`
* This method allows you to apply a function to every item in that column
* Any function called by this method will take as an argument the individual entries of that column
* Example use: Removing whitespace from a column

### Converting Files to `Table` Objects
`sqlify.text_to_sql(file, name, header, p_key, delimiter):`

`sqlify.csv_to_sql(file, name, header, p_key, delimiter=','):`
* skip_lines: Ignores the first `n` lines of data (default: 0)
    * Works independently of the `header` argument

### Converting `Table` objects to SQL databases
Once you are done cleaning the data, you can convert the finished result into an SQL database by using the following function:

`sqlify.table_to_sql(obj, database, name)`:
* obj: A Table object
* database: The name of the outputted sqlite3 database
* name: The name of the corresponding SQL table

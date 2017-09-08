Reading ZIP Archives
=====================

PGReaper provides an intutive way to access files stored in ZIP archives.
You can also pass the references to these files to PGReaper's normal reader functions
like `copy_csv()`.

Step 1: Read the ZIP Archive
------------------------------
.. autofunction:: pgreaper.read_zip

Step 2: Get the Specific File
--------------------------------------
.. autoclass:: pgreaper.io.zip.ZipFile

Notes: File Opening Safety
----------------------------
Opening a file within a ZIP archive using the methods above creates a 
`ZipReader` object. These objects are like any other file-like objects in Python--
supporting `read()` and `readline()` methods, but can only be used as context managers.

.. autoclass:: pgreaper.io.zip.ZipReader
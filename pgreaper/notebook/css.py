# Temporary until I figure out how to install custom CSS into Jupyter notebook

from pgreaper._globals import SQLIFY_PATH
import os

with open(os.path.join(SQLIFY_PATH, 'notebook', 'pgreaper.css'), 'r') as css:
    SQLIFY_CSS = ''.join(css.readlines()).replace('\n', '')
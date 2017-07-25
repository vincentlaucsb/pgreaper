# Temporary until I figure out how to install custom CSS into Jupyter notebook

from sqlify._globals import SQLIFY_PATH
import os

with open(os.path.join(SQLIFY_PATH, 'notebook', 'sqlify.css'), 'r') as css:
    SQLIFY_CSS = ''.join(css.readlines()).replace('\n', '')
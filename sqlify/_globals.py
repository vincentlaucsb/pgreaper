''' Global Constants '''

from importlib import import_module
import locale
import os
import sys

SQLIFY_PATH = os.path.dirname(__file__)
DEFAULT_ENCODING = locale.getpreferredencoding()

'''
Example
--------
>>> PYTHON_VERSION
3.6.0
'''
PYTHON_VERSION = sys.version_info[0] + 0.1 * sys.version_info[1] + \
    sys.version_info[2]

''' Optional Dependencies '''
def import_package(name):
    ''' Returns a package if it is installed, None otherwise '''
    try:
        return import_module(name)
    except ImportError:
        return None
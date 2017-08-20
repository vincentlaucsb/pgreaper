from sqlify._globals import SQLIFY_PATH

import copy
import functools
import os
import warnings

# Python 2.7 Compatibility
try:
    import configparser
except ImportError:
    import ConfigParser
    configparser = ConfigParser

# Store configuration file in sqlify's base directory
SQLIFY_CONF = configparser.ConfigParser()
SQLIFY_CONF_PATH = os.path.join(SQLIFY_PATH, 'config.ini')
SQLIFY_CONF.read(SQLIFY_CONF_PATH)

class DefaultSettings(dict):
    '''
    Sort of like a default dict
     * A factory for producing new dicts
     * If a key isn't specified or is None, it produces a dict with the 
       default value
    '''
    
    def __init__(self, section):
        try:
            # {k:SQLIFY_CONF[section][k] for k in SQLIFY_CONF[section]})            
            super(DefaultSettings, self).__init__()
            for k in SQLIFY_CONF[section]:
                if SQLIFY_CONF[section][k]:
                    self[k] = SQLIFY_CONF[section][k]
                else:
                    self[k] = None
        except KeyError:
            raise KeyError('There is no section named {} in the settings.'.format(
                section))

    def __call__(self, **kwargs):
        new_dict = copy.deepcopy(self)
        
        for k in kwargs:
            if (kwargs[k] is not None) and (k in self.keys()):
                new_dict[k] = kwargs[k]
                
        return new_dict
        
    # def __setitem__(self, key, value):
        # return self.__call__(key=value)

try:
    PG_DEFAULTS = DefaultSettings('postgres_default')
except KeyError:
    SQLIFY_CONF['postgres_default'] = {}
    SQLIFY_CONF['postgres_default']['user'] = 'postgres'
    SQLIFY_CONF['postgres_default']['password'] = ''
    SQLIFY_CONF['postgres_default']['host'] = 'localhost'
    SQLIFY_CONF['postgres_default']['dbname'] = 'postgres'
    
    PG_DEFAULTS = DefaultSettings('postgres_default')

    warnings.warn("No default Postgres settings found. Use"
        "sqlify.settings(user='', password='', dbname='', host='') to set them.")
        
def settings(hide=True, *args, **kwargs):
    '''
    Read, write, and modify configuration setttings. Currently,
    the only settings are for the default PostgreSQL database.
    
    **Arguments**
     * hide: Obfuscate password with asterisks
    
    **To view existing settings**
     
     >>> import sqlify
     >>> sqlify.settings()
     
    **To set new settings, or modify existing ones**
     * If creating settings for the first time, the `database`, `username`,
       and `password` arguments should be used
     * `hostname` will default to `localhost` if not specified
     * To modify existing settings, you only need to specify the setting
       you are changing.
 
     >>> import sqlify
     >>> sqlify.settings(database='postgres',
                         username='peytonmanning',
                         password='omaha',
                         hostname='localhost')
                         
    .. note:: This stores your username and password in a plain-text INI file.
    '''
    
    # List of keywords suggesting user wants to modify Postgres settings
    pg_kwargs = set(['user', 'password', 'host', 'dbname'])
    
    # No arguments --> Print settings
    if (not args) and (not kwargs):
        print_settings(hide)

    # Modify Postgres settings
    elif pg_kwargs.intersection( set( kwargs.keys() ) ):
        if 'postgres_default' not in SQLIFY_CONF.sections():
            first_time = True
            SQLIFY_CONF['postgres_default'] = {}
        else:
            first_time = False
        
        # Record values of arguments that aren't nonsense
        for key in pg_kwargs:
            try:
                SQLIFY_CONF['postgres_default'][key] = kwargs[key]
            except KeyError:
                if first_time and key != 'hostname':
                    # Require user to provide values for all keys first time
                    raise KeyError("Please specify a 'database', 'username' and 'password'. Optionally, you may also specify a 'hostname' (default: 'localhost').")
                    
                pass
        
        # If 'hostname' argument is missing, default to 'localhost'
        if 'hostname' in kwargs:
            SQLIFY_CONF['postgres_default']['host'] = kwargs['host']
        else:
            SQLIFY_CONF['postgres_default']['host'] = 'localhost'
            
        with open(SQLIFY_CONF_PATH, 'w') as conf_file:
            SQLIFY_CONF.write(conf_file)
            
        print_settings(hide)
            
    else:
        raise ValueError('Invalid argument. Valid keyword arguments are {}.'.format(pg_kwargs))
    
def print_settings(hide):
    ''' Print current user settings '''
    
    if SQLIFY_CONF.sections():
        for section in SQLIFY_CONF.sections():
            print('[{}]'.format(section))
            
            for key in SQLIFY_CONF[section]:
                val = SQLIFY_CONF[section][key]
                
                if key == 'password' and hide:
                    val = '{0} (Type sqlify.settings(hide=False) to show)'.format('*' * 15)
                
                print('{0}: {space} {1}'.format(
                    key, val,
                    space=' ' * (12 - len(key))))
    else:            
        print("No settings found.")
from sqlify._sqlify import alias_kwargs

import os
import warnings
import configparser

# Store configuration file in sqlify's base directory
SQLIFY_PATH = os.path.dirname(__file__)
SQLIFY_CONF = configparser.ConfigParser()
SQLIFY_CONF_PATH = os.path.join(SQLIFY_PATH, 'config.ini')
SQLIFY_CONF.read(SQLIFY_CONF_PATH)
    
@alias_kwargs
def settings(hide=True, *args, **kwargs):
    ''' Read and write configuration setttings
    
    * hide: Obfuscate password with *'s
    
    Option 1: No Arguments (except hide)
     --> Returns settings
     
    Option 2: Keyword Arguments (corresponding to default Postgres settings)
     * database
     * username
     * password
     * host
    '''
    
    # List of keywords suggesting user wants to modify Postgres settings
    pg_kwargs = set(['username', 'password', 'hostname', 'database'])
    
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
    
try:
    POSTGRES_DEFAULT_USER = SQLIFY_CONF['postgres_default']['username']
    POSTGRES_DEFAULT_PASSWORD = SQLIFY_CONF['postgres_default']['password']
    POSTGRES_DEFAULT_HOST = SQLIFY_CONF['postgres_default']['host']
    POSTGRES_DEFAULT_DATABASE = SQLIFY_CONF['postgres_default']['database']
except KeyError:
    warnings.warn("No default Postgres settings found. Use sqlify.settings(username='', password='', database='', hostname='') to set them.")
    
    POSTGRES_DEFAULT_USER = None
    POSTGRES_DEFAULT_PASSWORD = None
    POSTGRES_DEFAULT_HOST = None
    POSTGRES_DEFAULT_DATABASE = None
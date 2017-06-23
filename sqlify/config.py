import os
import warnings
import configparser

# Store configuration file in sqlify's base directory
SQLIFY_PATH =  os.path.dirname(__file__)

SQLIFY_CONF_PATH = os.path.join(SQLIFY_PATH, 'config.ini')
SQLIFY_CONF = configparser.ConfigParser()
SQLIFY_CONF.read(SQLIFY_CONF_PATH)

def settings(**kwargs):
    ''' Read and write configuration setttings
    
    Option 1: No Arguments
     --> Returns settings
     
    Option 2: Keyword Arguments (corresponding to default Postgres settings)
     * database
     * username
     * password
     * host
    '''
    
    # List of keywords suggesting user wants to modify Postgres settings
    pg_kwargs = set(['username', 'password', 'host', 'database'])
    
    # No arguments --> Print settings
    if (not args) and (not kwargs):
        if SQLIFY_CONF.sections():
            for section in SQLIFY_CONF.sections():
                print(section)
                
                for key in SQLIFY_CONF[section]:
                    print(key, SQLIFY_CONF[section][key])
        else:            
            print("No settings found.")

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
                if first_time:
                    # Require user to provide values for all keys first time
                    raise KeyError("Please specify a 'database', 'username' and 'password'. Optionally, you may also specify a 'host' (default: 'localhost').")
                    
                pass
        
        # If 'host' argument is missing, default to 'localhost'
        if 'host' in kwargs:
            SQLIFY_CONF['postgres_default']['host'] = kwargs['host']
        else:
            SQLIFY_CONF['postgres_default']['host'] = 'localhost'
            
        with open(SQLIFY_CONF_PATH, 'w') as conf_file:
            SQLIFY_CONF.write(conf_file)
            
    else:
        raise ValueError('What are you doing')
    
try:
    POSTGRES_DEFAULT_USER = SQLIFY_CONF['postgres_default']['username']
    POSTGRES_DEFAULT_PASSWORD = SQLIFY_CONF['postgres_default']['password']
    POSTGRES_DEFAULT_HOST = SQLIFY_CONF['postgres_default']['host']
    POSTGRES_DEFAULT_DATABASE = SQLIFY_CONF['postgres_default']['database']
except KeyError:
    warnings.warn('No default Postgres settings found.')
    
    POSTGRES_DEFAULT_USER = None
    POSTGRES_DEFAULT_PASSWORD = None
    POSTGRES_DEFAULT_HOST = None
    POSTGRES_DEFAULT_DATABASE = None
# Walks user through creating an SQL database

from .. import sqlify
from .. import utils

from functools import partial
import click

# To call, type `sqlify` from the command line
@click.command()
def cli():
    click.echo('Welcome to SQLify Version 1.0.1')
    click.echo('\n')
    click.echo('[1] Read the first few lines of a file')
    click.echo('[2] Convert a file to SQL')
    click.echo('...')
    click.echo('[0] Quit')
    click.echo('\n')
    
    response = click.prompt('Please enter an number to continue')
    
    if response == '1':
        head()
    elif response == '2':
        to_sql()
    elif response == '0':
        quit()
    else:
        click.echo('Invalid response. Enter "0" or Ctrl + C to quit.')
    
    cli()

def to_sql():
    # Ask for filename
    filename = click.prompt('Please enter a file', type=click.Path(exists=True))
    
    delims = ''' ' ':  Space-delimited
             '\t': Tab delimited
         '''            
    
    delimiter = click.prompt('How is the data delimited? Examples include: {0}'.format(delims))
    
    database = click.prompt('Enter a database to save to')
    
    # Use text_to_sql or csv_to_sql depending on the file extension
    file_ext = filename[-3:]
    
    if 'txt' == file_ext:
        func = sqlify.text_to_sql
    else:
        func = sqlify.csv_to_sql
        
    func(filename, database=database, delimiter=delimiter)
    
    click.echo('Created SQL database')
    
def head():
    filename = click.prompt('What is the filename?')
    delimiter = click.prompt('What is the delimiter?')
    
    # Print file
    print(utils.head(filename, n=10, delimiter=delimiter))
    
    # Ask for follow-up
    click.echo('[1] Read more lines')
    click.echo('[2] Convert a file to SQL')
    click.echo('...')
    click.echo('[0] Quit')
    option = click.prompt('What would you like to do next?')
    
    if option == '1':
        print(utils.head(filename, n=20, delimiter=delimiter))
    elif option == '2':
        to_sql()
    else:
        quit()
# Walks user through creating an SQL database

from .. import sqlify
from .. import utils
from .. import settings

import click
import os

# To call, type `sqlify` from the command line
@click.command()
def cli():
    click.echo('Welcome to SQLify Version 1.0.1')
    click.echo('')
    
    click.echo('='*75)
    click.echo('PostgresSQL Configuration Settings (from settings.py)')
    click.echo('Default User: ' + settings.POSTGRES_DEFAULT_USER)
    click.echo('Default Database: ' + settings.POSTGRES_DEFAULT_DATABASE)
    click.echo('Default Host: ' + settings.POSTGRES_DEFAULT_HOST)
    click.echo('Default Password: ' + '*'*len(settings.POSTGRES_DEFAULT_PASSWORD))
    click.echo('='*75)
    click.echo('')
    
    click.echo('[1] Read the first few lines of a file')
    click.echo('[2] Convert a file to SQL')
    click.echo('...')
    click.echo('[0] Quit')
    click.echo('')
    
    while True:
        response = click.prompt('Please enter an number to continue')
    
        if response.isnumeric():
            if response == '1':
                head()
            elif response == '2':
                to_sql()
                
            break
        else:
            click.echo('Invalid response. Enter "0" or Ctrl + C to quit.')
            click.echo('\n')
            
def to_sql(filename=None):
    # Ask for filename
    if not filename:
        filename = click.prompt('Please enter a file', type=click.Path(exists=True))
    
    delimiter = click.prompt('How is the data delimited (separated)?')
    database = click.prompt('Enter a database to save to')
    
    # Use text_to_sql or csv_to_sql depending on the file extension
    file_ext = filename[-3:]
    
    if 'txt' == file_ext:
        func = sqlify.text_to_sql
    else:
        func = sqlify.csv_to_sql
    
    while True:
        click.echo('Which database engine would you like to use?')
        click.echo('[1] SQLite')
        click.echo('[2] PostgresSQL')
        engine_choice = click.prompt('Enter a number')
        
        engines = {
            '1': 'sqlite',
            '2': 'postgres'
        }
        
        if engine_choice in engines:
            engine_str = engines[engine_choice]
            break
        else:
            click.echo('Invalid choice. Please choose again.\n')
    
    if engine_str == 'postgres':
        pg_settings = postgres_settings()
    else:
        pg_settings = {}
    
    # Create the database            
    func(file=filename, database=database, delimiter=delimiter,
        engine=engine_str, **pg_settings)
    
    click.echo('Created SQL database')
    
def head():
    filename = click.prompt('What is the filename?',
        type=click.Path(exists=True))
    
    # Give a short preview
    for i in utils.preview(filename, n=10):
        click.echo(i)
    
    # Ask for follow-up
    option = click.prompt('Would you like to conver this file to SQL? [y/n]')
    
    if option.lower() == 'y':
        to_sql(filename)
    else:
        quit()
        
# Prompt user for input on connecting to a Postgres database
def postgres_settings():
    username = click.prompt(
        "What username do you want to connect with?\n" +
        "(Leave blank to use {0})".format(settings.POSTGRES_DEFAULT_USER))
        
    password = click.prompt(
        "What password do you want to connect with?\n" +
        "(Leave blank to use default)",
        hide_input=True, confirmation_prompt=True)
        
    return {'username': username, 'password': password}
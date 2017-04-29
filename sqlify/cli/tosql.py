# Convert a TXT file to SQL

from .. import sqlify

import click

@click.command()
@click.option('--delim', default=' ', help='Delimiter of the data')
@click.argument('filename')
@click.argument('database')
def cli(delim, filename, database):
    if 't' in delim:
        delim = '\t'

    sqlify.text_to_sql(file=filename,
                       database=database,
                       delimiter=delim)
                       
    click.echo('Created SQL database')/
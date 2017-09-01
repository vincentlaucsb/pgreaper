''' Command Line Interface for non-PostgreSQL Goodies and Extras '''
import click

@click.command()
@click.option('--zip', 'zip', flag_value=True, default=False)
@click.option('--text', 'text', flag_value=True, default=False,
    help='Copy a TXT or other tab-separated file')
@click.option('--csv', 'csv', flag_value=True, default=False,
    help='Copy a CSV or other delimiter separated file')
@click.option('--preview', 'preview', flag_value=True, default=False,
    help='Preview how PGReaper will interpret a file instead of loading it')
@click.option('--delim', default=',', help='Delimiter')
@click.option('--header', default=0, help='Line number of the header row', show_default=True)
@click.option('--skiplines', default=0, help='Number of lines after the header to skip', show_default=True)
@click.argument('filename', nargs=-1)
def datareaper():
    ''' For non-Postgres related goodies '''
    pass
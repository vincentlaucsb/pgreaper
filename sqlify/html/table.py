from sqlify.core.tabulate import Tabulate

def html_table(n_cols, engine='sqlite', *args, **kwargs):
    ''' Returns an HTML table '''

    if 'col_names' not in kwargs:
        kwargs['col_names'] = ['col{}'.format(i) for i in range(0, n_cols)]
    
    if 'name' not in kwargs:
        kwargs['name'] = None
        
    return Tabulate.factory(engine, n_cols=n_cols, *args, **kwargs)
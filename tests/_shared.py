''' Objects useful for various tests '''
from sqlify import Table

def world_countries_cols():
    return ['Capital', 'Country', 'Currency', 'Demonym', 'Population']
                   
def world_countries():
    return [["Washington", "USA", "USD", 'American', "324774000"],
            ["Moscow", "Russia", "RUB", 'Russian', "144554993"],
            ["Ottawa", "Canada", "CAD", 'Canadian', "35151728"]]
            
def world_countries_table():
    return Table('Countries',
        col_names = world_countries_cols(),
        row_values = world_countries()
    )
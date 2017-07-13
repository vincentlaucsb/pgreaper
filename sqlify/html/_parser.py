''' Helper functions for html.parser '''

from collections import Counter
from copy import copy, deepcopy

def count_cols(rows):
    ''' Given a sample (list) of several rows, count the number of columns '''
    
    n_cols_counts = []
    
    for row in rows:
        n_cols = 0
        
        for cell in row['children']:
            if cell['tag'] in ['td', 'th']:
                try:
                    n_cols += cell['colspan']
                except KeyError:  # No colspan
                    n_cols += 1
                    
        n_cols_counts.append(n_cols)

    # Return the majority vote
    return Counter(n_cols_counts).most_common(n=1)[0][0]
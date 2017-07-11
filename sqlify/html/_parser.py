''' Helper functions for html.parser '''

def handle_th(tbl, node):
    ''' Node should be a table row containing th's '''
    
    i = 0
    
    for cell in node['children']:
        if 'colspan' in cell.keys():
            if cell['colspan'] == tbl.n_cols:
                # TH spans entire table... probably a table name
                tbl.name = cell.get_data()
            else:
                # Make column name repeat
                col_name = cell.get_data()
                
                for j in range(0, cell['colspan']):
                    # TO DO: Refactor Table to get rid of this trash
                    try:
                        tbl.col_names[i] = col_name
                    except TypeError:
                        # col_names is None
                        tbl.col_names = [cell.get_data()]
                    except IndexError:
                        tbl.col_names.append(cell.get_data())
                        
                    i += 1
                
        else:
            # TO DO: Rewrite this to be more elegant
            try:
                tbl.col_names[i] = cell.get_data()
            except TypeError:
                # col_names is None
                tbl.col_names = [cell.get_data()]
            except IndexError:
                tbl.col_names.append(cell.get_data())
                
            i += 1
        
    tbl.col_names = [cell.get_data() for cell in node['children']]
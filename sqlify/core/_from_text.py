def clean_line(line, table):
    '''
    Take in a line of strings and cast them to the proper type
    
    Parameters
    -----------
    line:       list
                List of strings from CSV reader
    table:      Table
    '''

    new_line = []
    
    for i in line:
        j = i.replace(' ', '')

        if not j.isalnum():
            new_line.append(i)
        elif j.isnumeric():
            new_line.append(int(j))
        elif (j[0] == '-') and j.count('.') <= 1:
            new_line.append(float(j))
        else:
            new_line.append(i)
            
    table.append(new_line)
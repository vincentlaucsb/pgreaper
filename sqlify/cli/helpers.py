# Helper functions for the CLI module

import click

def _hrule(text):
    '''
        ===================== Goal ====================
        ===== Print text inside a horizontal rule =====
        
    '''
    
    bar_width = int((75 - len(text) + 2)/2)
    return '='*bar_width + ' ' + text + ' ' + '='*bar_width

def _to_pretty_table(lst):
    '''
    Goal:
     * Take a list of strings and print them in a pretty table
     
    Return:
     * Should be a giant string with "\n"'s
    '''
    
    ret_str = ''
    
    # 5 column names/row
    n = 0
    
    for name in lst:
        ret_str += '  {:^15}  '.format(name)
        n += 1
        
        if (n % 5 == 0):
            ret_str += '\n'
    
    return ret_str

def _validate_response(prompt, valid, text=[], default=None):
    '''
    Goal:
     1. Displays lines of text to the user
     2. Presents the prompt
     3. Ensures it's valid
     4. If invalid, repeat prompt
      a. Otherwise, return choice
      
    Arguments:
     * prompt:  A string for the final prompt
     * valid:   A collection of valid responses
     * text:    (Optional) A list of text to be displayed before the prompt
     * default: Default option
    '''

    if text:
        for line in text:
            click.echo(line)

    if default:
        choice = click.prompt(prompt, default=default)
    else:
        choice = click.prompt(prompt)

    # So I don't have to do valid=['1', '2']
    if (choice in valid) or (int(choice) in valid):
        return choice
    else:
        click.echo('Invalid choice. Please choose again.\n')
        _validate_response(prompt, valid, text)
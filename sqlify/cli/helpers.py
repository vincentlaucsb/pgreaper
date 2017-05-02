# Helper functions for the CLI module

import click

# Display parameters
TEXT_WIDTH = 75
TABLE_WIDTH = 120

# Echo a horizontal line
def echo_hrule(text='', wide=False):
    '''
    ===================== Goal ====================
    ===== Print text inside a horizontal rule =====
        
    =================== Arguments =================
     * wide:    Make rule TABLE_WIDTH wide
    '''
    
    t_width = TEXT_WIDTH
    if wide:
        t_width = TABLE_WIDTH
    
    bar_width = int((t_width - len(text) + 2)/2)
    
    if text:
        click.echo('{bar} {text} {bar}'.format(bar='=' * bar_width, text=text))
    else:
        click.echo('=' * t_width)

# Echo (Trimmed)
def trim(text):
    '''
     * Trim text to TEXT_WIDTH characters per line
     * Don't cut split words in the middle
     * Overflow goes on next line
    '''
    
    new_text = ''
    
    # Assume words are separated by spaces
    current_word = ''
    
    for chr in enumerate(text):
        if (chr[0] > 0) and (chr[0] % TEXT_WIDTH == 0):
            new_text += '\n'
            current_word += chr[1]
        else:
            if chr[1] == ' ':
                new_text += current_word + ' '
                current_word = ''
            else:
                current_word += chr[1]
                
    # Add remaining text at end
    new_text += current_word
                
    return new_text
    
def echo_trim(text):
    click.echo(trim(text))
    
def echo_table(lst):
    ''' Take a list of strings and print them in a pretty table '''
    
    ret_str = ''
    n = 0  # 5 column names/row
    
    for name in lst:
        ret_str += '  {:^15}  '.format(name)
        n += 1
        
        if (n % 5 == 0):
            ret_str += '\n'
    
    click.echo(ret_str)

def _validate_menu(context, option, value):
    if value not in [0, 1, 2, 9]:
        raise click.BadParameter('Please enter a valid option')
    else:
        return value

def _validate_yes_no(context, option, value):
    if value.lower() not in ['y', 'n']:
        raise click.BadParameter('Please enter y or n')
    else:
        return value
        
def validate_prompt(prompt, valid, default=None):
    '''
    Prompts a user and checks reprompts if the response is invalid
    
    Arguments:
     * prompt:  A string or list of strings for the final prompt
     * valid:   A collection of valid responses
     * default: Default option
    '''

    if isinstance(prompt, list):
        for line in prompt[0:-1]:
            click.echo(line)
            
        prompt = prompt[-1]
            
    choice = click.prompt(prompt, default=default)

    # So I don't have to do valid = ['1', '2']
    valid = [str(choice) for choice in valid]
    
    if choice in valid:
        return choice
    else:
        click.echo('Invalid choice. Please choose again.\n')
        validate_prompt(prompt, valid, default)
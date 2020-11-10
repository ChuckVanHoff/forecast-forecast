''' You can used these scripts for handling errors during API requests. '''

import requests


def retry(command, *args):
    ''' This takes the command and tries and retries to complete it, each
    time keeping a lookout for the coded errors.
    
    Returns a -1 flag if it cannont get past with a few tries.
    '''
    
    n = 0
    while n < 4:
        try:
            result = command(*args)
            return result
        except requests.exceptions.ReadTimeout as e:
            print(f'There was an exception in the {n}th cycle: {e}')
            n += 1
    print(f'tried 3 times with {command} and {args}')
    return -1
    
def keep_going(command, *args):
    ''' Take command and args and perform the operation while looking out for
    the errors.
    
    Returns a -1 flag if there was an error that could not be resolved.
    '''
    
    try:
        result = command(*args)
        return result
    except requests.exceptions.ReadTimeout as e:
        print(f'There was an exception: {e}')
    return -1

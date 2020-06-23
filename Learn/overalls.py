''' general use functions and classes '''

import collections

def read_list_from_file(filename):
    """ Read the zip codes list from the csv file.
        
    :param filename: the name of the file
    :type filename: sting
    """
    with open(filename, "r") as z_list:
        return z_list.read().strip().split(',')
    
def key_list(d=dict):
    ''' Write the dict keys to a list and return that list 
    
    :param d: A python dictionary
    :return: A list fo the keys from the python dictionary
    '''
    
    keys = d.keys()
    key_list = []
    for k in keys:
        key_list.append(k)
    return key_list

def all_keys(d):
    ''' Get all the dicitonary and nested "dot format" nested dictionary keys
    from a dict, add them to a list, return the list.
    
    :param d: A python dictionary
    :return: A list of every key in the dictionary
    '''
    keys = []    
    for key, value in d.items():
        if isinstance(d[key], dict):
            for sub_key in all_keys(value):
                keys.append(f'{key}.{sub_key}')
        else:
            keys.append(str(key))
    return keys

### THIS DOES NOT WORK ###
# def all_values(d, values=[]):
#     ''' Get all the dicitonary and nested dictionary keys from a dict, add them
#     to a list, return the list.
    
#     :param d: A python dictionary
#     :return: A list of every key in the dictionary
#     '''

#     for key, value in d.items():
#         if isinstance(d[key], dict):
#             all_values(d[key], values=values)
#         else:
#             values.append(value)
#     return values

def update_nested(d, u):
    ''' Update a dict at any key, nested or top-level.
    
    :param d: the dictionary to be updated
    :param u: the dictionary to provide the updates
    '''
    
    for k, v in u.iteritems():
        if isinstance(d, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        else:
            d = {k: u[k]}
    return d

### I don't know if this funciton works as it has not been thoroughly tested.
### It's here because I thought that maybe I wanted the feature that includes
### the key checks on both dicts, but I got to thinking that I don't know what
### the key check adds to functionality.
def update_all(dict_a, dict_b):
    ''' Update a nested dict. Check keys in both dicts.
    
    :param dict_a: the dict to update
    :param dict_b: the dict with the updates
    '''
    
    set_keys = set(dict_a.keys()).union(set(dict_b.keys()))
    for k in set_keys:
        v = dict_a.get(k)
        if isinstance(v, dict):
            new_dict = dict_b.get(k, None)
            if new_dict:
                update_nested(v, new_dict)
        else:
            new_value = dict_b.get(k, None)
            if new_value:
                dict_a[k] = new_value

def compare_dicts(one, the_other, diff={}):
    ''' Compare the values of two dicts, key by common key. When the values are
    numbers return the difference, when strings return 0 if the strings are
    equal and 1 if they are different, when dicts run this function, when
    NoneType set the value to None.

    :params one, the_other: dictionaries with the same set of keys and sub-keys
    :type one, the_other: dict
    '''
    
    diff = {}
    
    for k in one:
        # check if one is a dict: if it is then re-run the compare function
        if isinstance(one[k], dict):# == dict:
            print(one[k], the_other[k])
            diff[k] = compare_dicts(one[k], the_other[k], diff=diff)
       
        # Check for the presence of the key in the_other: add it to the_other
        # if it's not already there, and set the key to some string.
        if not the_other.get(k):
            the_other[k] = '0000000000'
            continue

        # Check and compare dictionaries according to their value type
        try:
            # See if the values are numbers.
            if type(one[k]) == int or type(one[k]) == float:
                if type(the_other[k]) == int or type(the_other[k]) == float:
                    diff[k] = round( one[k]-the_other[k], 2)
            elif type(one[k]) == str:
                if one[k] == the_other[k]:
                    diff[k] = 0
                else:
                    diff[k] = 1
        # If for some reason the key is still not in the the_other, add it.
        except KeyError as e:
            # Insert whatever key it is that is missing from the dict.
            the_other[e.args[0]] = '0000000000'
            print(f'added new key to the_other with value 0000000000 key= {e}')
    
    # Now check the_other for any keys that are missing from one.
    # Add the key if it is missing.
    for k in the_other.keys():
        if k not in one.keys():
            one[k] = '0000000000'
    return diff

def flatten(d):
### I'm not 100% sure the order of the dict keys and values is preserved? ###
    ''' Flatten a dict by zipping together a list of keys and nested keys and
    a list of each key value.
    
    :param d: a dictionary
    '''
    
    keys = all_keys(d)
    values = all_values(d)
    return dict(zip(keys, values))

### Thanks to geeksforgeeks.org for the function. ###
def flatten_dict(dd, separator ='.', prefix =''):
    ''' Code to convert dict to flattened dictionary. '''
    return { prefix + separator + k if prefix else k : v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
           } if isinstance(dd, dict) else { prefix : dd }


### Thanks to geeksforgeeks.org for the function. ###
def convert_flatten(d, parent_key ='', sep ='.'):
    ''' Code to convert a dict to a flattened dictionary. '''
    from collections.abc import MutableMapping
    
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(convert_flatten(v, new_key, sep = sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
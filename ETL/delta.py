''' Create the "delta" documents. These should have the same fields as those in
the remote database, owmap.legit_inst, but values should be the difference 
between the document.observation and document.forecast[i]. 

get an instant document. loop through the forecasts array. each loop, create a 
delta that has a Weather.as_dict structure with delta values at each key. add
each of those deltas from the weathers array to another, maybe called
"deltas"? ....and more. '''


class Fdiff:#(cast, obs):
    ''' a delta document between a forecast and its observation. '''
    
    ### you need to get an Instant document from the remote database ###
    
    ### create a loop over a pair of weather dicts to set the value of each
    ### key in the weather dict equal to the difference between the two key
    ### values


def make_delta(cast, obs):
    ''' MODIFIED FROM OVERALLS.COMPARE_DICTS()
    Compare the values of two dicts, key by key. When the values are numbers
    return the difference, when strings return 0 if the strings are equal and 1
    if they are different, when dicts run this function, when NoneType set the
    value to 99999.
    
    :params cast, obs: dictionaries with the same set of keys and sub-keys
    :type cas, obs: dict
    '''
    
    delta = {}  # The delta document. Contains all the forecast errors
    
    for (k, v) in cast.items():
        try:
            # Check and compare dictionaries according to their value type
            if type(v) == int or type(v) == float:
                if type(obs[k]) == int or type(obs[k]) == float:
                    delta[k] = v - obs[k]
            elif type(v) == dict:
                delta[k] = make_delta(v, obs[k])
            elif type(v) == str:
                if v == obs[k]:
                    delta[k] = 0
                else:
                    delta[k] = 1
            elif type(v):
                delta[k] = 999999
        except KeyError as e:
            print(f'Caught a KeyError..... {e}')
################### Modified from compare_dicts() in overalls.py ##############
            # Add whichever key and value needs adding to the delta
            if k not in obs and '1h' in obs:
                delta['1h'] = obs['1h']
                delta[k] = v
            elif k not in obs and '3h' in obs:
                delta['3h'] = obs['3h']  
                delta['1h'] = v
###############################################################################
    return delta
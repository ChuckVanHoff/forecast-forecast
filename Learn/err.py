import overalls

def errors(casts, obs):
    ''' Make a dict of errors for the forecasts.
    
    :param casts: a list of dictionaries
    :param obs: a dictionary
    
    * For best results all dicts should have all the same keys and subkeys.
    '''
    
    # Flatten all dicts and compare. Add the comparisons to a list and return.
    casts = [overalls.flatten_dict(cast) for cast in casts]
    obs = overalls.flatten_dict(obs)
    return [overalls.compare_dicts(cast, obs) for cast in casts]

def gen_errs_df(df):
    ''' Create an errors dataframe from the argument.
    
    :param df: Must be a pandas DataFrame with flat dict entries.
    '''
    ### is there a way to step through three lists together? ###
    errs_list = []
    errs_dict = {}

    # Create the error dicts list to be added to the errs_dict.
    for (obs, casts) in zip(df['weather'], df['forecasts']):
        errs_list.append(errors(casts, obs))
    
    # Create a dict from the errors list with the indexes as keys and transform
    # to a DataFrame
    for (_id, errs) in zip(df['_id'], errs_list):
        errs_dict[_id] = errs
    dd = pd.DataFrame.from_dict(errs_dict, orient='index') ### this puts that dict into DataFrame form
    
    # Replace the errors DataFrame dictionaries with a list of their values 
    for c in dd.columns:
        dd[c] = [list(d.values()) for d in dd[c]]
    return dd

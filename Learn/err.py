''' Make an errors csv file. '''

import pandas as pd
from pymongo import MongoClient

import config
import overalls
import weather
import benedict


def read_mongo_to_df(uri, db, collection, query={}, limit=None):
    """ Read from Mongo and Store into DataFrame """

    con = MongoClient(uri)
    db = con[db]
    col = db[collection]
    # Make a query to the specific DB and Collection
    if limit:
        cursor = col.find(query)
        cursor = cursor[limit]
        print(f'number of indexes created has been limited to {limit} .......')
    else:
        cursor = con[collection].find(query)
    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame.from_dict(cursor, orient='index')
    return df.transpose()

def fill_missing(row):
    ''' This function makes sure that there are no missing values in the dicts
    of the DataFrame. It converts each of them to a weather.Weather object and
    replaces the db contents with the object data.
    
    Use it with the .apply() method.
    '''
  
    row['weather'] = weather.Weather(row['zipcode'], \
                                     'observation', \
                                     row['weather']).weather
    for cast in row['forecasts']:
        cast = weather.Weather(row['zipcode'], 'forecast', cast).weather
    return row

def errors(casts, obs):
    ''' Make a dict of errors for the forecasts. Any dicts in the arguments
    will be flattened before comparison.
    
    :param casts: a list of dictionaries
    :param obs: a dictionary
    
    * For best results all dicts should have all the same keys and subkeys.
    '''
    
    # Flatten all dicts and compare. Add the comparisons to a list and return.
    casts = [overalls.flatten_dict(cast) for cast in casts]
    obs = overalls.flatten_dict(obs)
    return [overalls.compare_dicts(cast, obs) for cast in casts]


if __name__ == '__main__':
    collection = 'legit_inst'
    df = read_mongo_to_df(config.uri, config.database, collection, limit=5)
    
    # Clean the df up a little bit
    df = df.apply(fill_missing, axis=1)
    for w in df['weather']:
        w.pop('sunset_time', 'sunrise_time')
        w.pop('sunrise_time')
    for l in df['forecasts']:
        for f in l:
            f.pop('sunset_time', 'sunrise_time')
            f.pop('sunrise_time')
            
    # Create the errors DataFrame.
    errs = []
    for index, row in df[['forecasts', 'weather']].iterrows():
         errs.append(errors(row['forecasts'], row['weather']))
    df['errs'] = errs
    err_vals = overalls.strip_keys(df['errs'])
    cast_vals = overalls.strip_keys(df['forecasts'])
    dd = pd.DataFrame([cast_vals, err_vals], index=['forecasts', 'errors'])
    dd = dd.transpose()
    
    filename = 'error_set.csv'
    dd.to_csv(filename, float_format='%.2f')

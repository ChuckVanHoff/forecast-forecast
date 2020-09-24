import numpy as np
import pandas as pd
import benedict

import config


client = config.client#MongoClient()
db = client[config.database]
col = db[config.instants_collection]

    
def update_keys(ref, check, as_kv_list=False):
    ''' Make sure that the keys for check are the same as those in ref.
    
    :param ref: the dictionary whose keys are to be referenced
    :type ref: dict
    :param check: the dict to have its keys checked and uppdated
    :type check: dict
    '''
    
    if not as_kv_list:
        keys1 = ref.keys()
        keys2 = check.keys()
    else:
        keys1 = [tup[0] for tup in ref]
        keys2 = [tup[0] for tup in check]
    diff1 = keys2 - keys1  #keys in dict2 that are not in dict1
    diff2 = keys1 - keys2  #keys in dict1 that are not in dict2
    for item in diff1:
        check.pop(item)
    for item in diff2:
        check[item] = None
    return

def strip_keys(df):
    ''' Take a pandas.DataFrame and replace each dict with the list of
    its values.
    '''
    
    t = []
    
    def dict_strip(x):
        ''' Strip the keys from a dict. '''
        
        if isinstance(x, dict):
            return [x for x in x.values()]
        else:
            return x
        
    for row in df.iterrows():
        temp = []
        for item in row[1]:
            
            temp.append(dict_strip(item))
        t.append(pd.Series(temp, name=row[1].name, dtype=object))
    return pd.concat(t, axis=1, ignore_index=False).transpose()

def compare_dicts(one, the_other, return_type='dict', as_kv_list=False):
    ''' Compare the values of two dicts, key by common key. When the values are
    numbers, return the difference: when strings, return 0: if the strings are
    equal, 0, 1 if they are different: when dicts, run this function: if it's a
    list then step through it, running this function on each element: when
    NoneType, set it to a flag value.

    :params one, the_other: dictionaries with the same set of keys and sub-keys
    :type one, the_other: dict
    '''
    
    delta = {}  # The delta document. Contains all the forecast errors
    
    if as_kv_list:
        for (k, v) in one:
            try:
                # Check and compare dictionaries according to their value type
                if type(v) == int or type(v) == float:
                    if type(the_other[k]) == int or type(the_other[k]) == float:
                        delta[k] = v - the_other[k]
                elif type(v) == dict:
                    delta[k] = compare_dicts(
                        v, the_other[k], return_type='list', as_kv_list=True)
                elif type(v) == str:
                    if v == the_other[k]:
                        delta[k] = 0
                    else:
                        delta[k] = 1
                elif type(v) == list:
                    delta[k] = [
                        compare_dicts(item, other_item)
                        for item, other_item
                        in list(zip(v, the_other[k]))
                    ]
                elif type(v):
                    delta[k] = 0
            except KeyError as e:
                print(f'missing key..... {e}')
    else:
        for (k, v) in one.items():
            try:
                # Check and compare dictionaries according to their value type
                if type(v) == int or type(v) == float:
                    if type(the_other[k]) == int or type(the_other[k]) == float:
                        delta[k] = v - the_other[k]
                elif type(v) == dict:
                    delta[k] = compare_dicts(v, the_other[k], return_type='list')
                elif type(v) == str:
                    if v == the_other[k]:
                        delta[k] = 0
                    else:
                        delta[k] = 1
                elif type(v) == list:
                    delta[k] = [
                        compare_dicts(item, other_item)
                        for item, other_item
                        in list(zip(v, the_other[k]))
                    ]
                elif type(v):
                    delta[k] = 0
            except KeyError as e:
                print(f'missing key..... {e}')
    if return_type == 'dict':
        return delta
    if return_type == 'list':
        return [v for v in delta.values()]

def find_item_with_kv_pair(series, key, value):
    '''Find and return the first item in a given pandas Series that has the
    given key-value pair.
    
    :param series: a pandas series
    :type series: pandas.Series
    :param key: the key the function should search for
    :type key: str
    :param value: the value the function should compare to
    :type value: I think anything that '==' can be used with
    
    :returns: the object found or None or raises TypeError
    '''
    
    if isinstance(series, pd.Series):
        for item in series:
            if isinstance(item, dict):
                if key in item:
                    if item[key] == value:
                        return item
            elif isinstance(item, list):
                for elem in item:
                    if elem[0] == key:
                        if elem[1] == value:
                            return item
        return None
    else:
        raise TypeError("find_item_with_key() wants a pandas.Series.")
        return

def tups_to_dict(tups):
    ''' Convert a list of tuples to a dictionary. '''
    dicti = {}
    for a, b in tups:
        dicti.setdefault(a, b)
    return dicti 

def read_mongo_to_df(collection, filters={}, limit=None):
    ''' Read a MongoDB cursor to a pandas DataFrame.
    Arguments are "collection", which must be a MongoDB
    client.database.collection object, and "filters", which
    can be a well formed mongo query. "limit" will limit
    the number of documents returned on the cursor.
    '''

    documents = collection.find(filters)[:limit]
    return pd.DataFrame.from_records([doc for doc in documents])

def records_to_rows(col, filters={}, limit=100):
    ''' Request records from the database collection and convert it to a
    pandas.DataFrame. All records are set with keys as column names and
    '_id' as the index.
    '''
    
    docs = col.find(filters)[:limit]
    weathers = pd.DataFrame()
    for row in docs:
        if isinstance(row, dict):
            for v in row.values():
                if isinstance(v, list):
                    row['weather'] = row['weather'][0]#v[0]
            # These next lines convert the dicts to benedicts before flattening,
            # sorting by keys, and then converting back to dicts.
            bene = benedict.benedict.flatten(row)
            flat_bene = benedict.benedict(bene)
            sorted_flat_bene = flat_bene.items_sorted_by_keys()
            sorted_flat_dict = tups_to_dict(sorted_flat_bene)
        weather = pd.DataFrame.from_dict(sorted_flat_dict, orient='index')
        weathers = pd.concat([weathers, weather.transpose()])
    return weathers

def read_mongo_a(col, filters={}, limit=None):
    ''' Retrieve data from the Mongo database and transform it to a pandas
    DataFrame; return the DataFrame.

    :param col: the MongoDB collection to be read
    :type collection: pymongo.collection.Collection
    :param filters: a well formed MongoDB query
    :type filters: dict
    :param limit: optional limiter to the number of documents retrieved
    :type limit: int
    '''

    # Shorten the cursor length if limit is given, otherwise get everything;
    # transform the retrieved data to a pandas.DataFrame and return it.
    docs = col.find(filters)[:limit]
    weathers = []
    for doc in docs:
        if isinstance(doc, dict):
            for v in doc.values():
                if isinstance(v, list):
                    doc['weather'] = doc['weather'][0]
        # Convert the dict to a benedict, flatten it, sort it, convert it back
        # to a dict, and finally transform the dict to a DataFrame and append
        # it to a list to tbe concatted to together.
        bene = benedict.benedict(doc).flatten().items_sorted_by_keys()
        dic = tups_to_dict(bene)
        df = pd.DataFrame.from_dict(dic, orient='index')
        weathers.append(df.transpose())
    if limit:
        print(f'The length of your df has been limited to {limit}.')
    return pd.concat(weathers)
    
def make_inst(df):
    ''' Create instant Series from the DataFrame: step through each row of the
    DataFrame and check the count of the row. If it is 42 or more, drop any na
    values, flatten each dict and append the Series to a new DataFrame and
    return it.
    '''
    
    instants = []
    for row in df.iterrows():
        if row[1].count() <= 39:
            continue
        row[1].dropna(inplace=True)
        obs = find_item_with_kv_pair(row[1], 'type', 'obs')
        for item in row[1].iteritems():
            if isinstance(item[1], dict):
                for v in item[1].values():
                    if isinstance(v, list):
                        item[1]['weather'] = item[1]['weather'][0]
                if obs != None:
                    if item[1]['type'] == 'cast':
                        update_keys(item[1], obs)
            if isinstance(item[1], list) and obs != None:
                if item[1][0] == 'cast':
                    update_keys(item[1], obs)
        # These next lines convert the dicts to benedicts before flattening,
        # sorting by keys, and then converting back to dicts.
        flat_data = row[1].apply(benedict.benedict.flatten)
        sorted_items = flat_data.apply(benedict.benedict.items_sorted_by_keys)
        flat_sorted_data = sorted_items.apply(tups_to_dict)
        instants.append(flat_sorted_data)
    instants = pd.concat(instants, axis=1, ignore_index=False).transpose()
    np.save('instants.npy', instants)
    return instants

def make_data_df(df):
    ''' Create the DataFrame that will contain the data to be used as the
    Data dataset to go along with the Target dataset. First make the instants
    DataFrame, then go through it row by row and remove all the items that
    are observation data. Finally save.
    '''
    
    data = []
    for row in df.iterrows():
        obs = find_item_with_kv_pair(row[1], 'type', 'obs')
        for item in row[1].iteritems():
            if isinstance(item[1], dict):
                
                if obs != None:
                    if item[1]['type'] == 'obs':
                        row[1].pop(item[0])
                    
        data.append(row[1])
    data_df = pd.concat(data, axis=1, ignore_index=False)
    data_df = strip_keys(data_df)
    np.save('forecast_values.npy', data_df)
    return data_df.transpose()
    

def make_deltas(series):
    ''' Take a pandas.Series and compare each of the items to one of the other
    items (dict comparisons) and return a pandas.Series of comparison results.
    '''

    deltas = []
    obs = find_item_with_kv_pair(series, 'type', 'obs')
    for item in series:
        if isinstance(item, dict) and obs != None:
            if item['type'] == 'cast':
                update_keys(item, obs)
                deltas.append(compare_dicts(obs, item, return_type='list'))
        if isinstance(item, list) and obs != None:
            if item[0] == 'cast':
                update_keys(item, obs)
                deltas.append(compare_dicts(obs, item, return_type='list'))
    return pd.Series(deltas, name=series.name, dtype=object)

def make_deltas_df(df):
    ''' Build the complete deltas DataFrame. '''
    
    deltas = []
    deltas_df = pd.DataFrame()
    
    # Create a DataFrame of the delta documemnts derived from the rows of
    # the supplied DataFrame. Add the DataFrame to a list so that it all
    # concatinates to a DataFrame. Then, row by row create the "deltas" for
    # the data and add it to the list. Finally concat all that together.
    deltas.append(deltas_df)
    for row in df.iterrows():
        deltas.append(make_deltas(row[1]))
    deltas_df = pd.concat(deltas, axis=1, ignore_index=False).transpose()
    np.save('delta_values.npy', deltas_df)
    return deltas_df

if __name__ == "__main__":
    df = read_mongo_to_df(col, limit=10000).set_index('_id')
    inst = make_inst(df)
    make_data_df(inst).head(1)
    make_deltas_df(inst).head(1)
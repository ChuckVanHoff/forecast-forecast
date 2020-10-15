''' Make the DataFrame containing all the errors in the weather forecasts as
compared to the reported observations. '''


import numpy as np
import pandas as pd
import benedict

import config
import pinky


# Setup the database connection and name the collection so do_diff_process()
# can be run.
client = config.client
db = client[config.database]
col = db[config.weathers_collection]


def tups_to_dict(tups):
    ''' Convert a list of two-tuples to a dictionary. '''

    dicti = {}
    for a, b in tups:
        dicti.setdefault(a, b)
    return dicti

def timeplaces(col, only_legit=False):
    ''' Make a list of timeplaces from the collection.
    
    :param col: The database collection to query.
    :type col: pymongo.collection.Collection
    :param only_legit: Determines whether or not to return all timeplaces
    or only the legit ones.
    :type only_legit: bool
    '''
    
    raw = col.find({})
    timeplaces = [doc['timeplace'] for doc in raw if doc['type'] == 'obs']
    if only_legit:
        legits = []
        for doc in timeplaces:
            tp = col.find({'timeplace': doc})
            if col.count_documents({'timeplace': doc}) >= 41:
                legits.append(doc)
        return legits
    return timeplaces

def records_to_rows(col, filters={}, limit=100, as_list=False):
    ''' Request records from the database collection and convert them to a
    list of pandas.DataFrames. All records are set with keys as column names
    and '_id' as the index.
    
    :param col: The mongodb collection to query
    :type col: pymongo.collection.Collection
    :param filters: The MongoDB query filter.
    :type filters: dict as a valid MongoDB search query.
    :param limit: The maximum number of documents to return from the return
        from the search query.
    :type limit: int
    :param as_list: An indicator to have the function return a list of
        DataFrames without concatenating them.
    :type as_list: bool
    '''
    
    docs = col.find(filters, batch_size=1000)[:limit]
    temp = []
    for row in docs:
        if isinstance(row, dict):
            # Lookout for the occurance of a list and handle appropriately.
            for v in row.values():
                if isinstance(v, list):
                    row['weather'] = row['weather'][0]
            # These next lines convert the dicts to benedicts before
            # flattening, sorting by keys, and then converting back to dicts.
            bene = benedict.benedict.flatten(row)
            flat_bene = benedict.benedict(bene)
            sorted_flat_bene = flat_bene.items_sorted_by_keys()
            sorted_flat_dict = tups_to_dict(sorted_flat_bene)
            # Store in temp list as a pandas.DataFrame.
            temp.append(pd.DataFrame(sorted_flat_dict, index=[row['_id']]))
    if as_list:
        return temp
    return pd.concat(temp)

def flatten_to_single_row(df):
    ''' A function to convert a DataFrame to a single row DataFrame.
    This function takes each row of the dataframe and represents it as a
    single DataFrame row with a given index made by the string concatenation
    of the row number and the column name.
    
    :param df: the dataframe to be flattened
    :type df: pandas.DataFrame
    '''

    df.reset_index(inplace=True)
    index = []
    data = []
    # Create corrosponding lists of index names and data items that will be
    # used to create a dataframe.
    for row in df.iterrows():
        for d, i in zip(row[1], row[1].index):
            index.append(str(i)+str(row[0]))
            data.append(d)
    d = pd.DataFrame(data, index=index)
    return d.transpose()

def make_instant_from_db(timeplace, collection=None, drop_cols=None, return_list=False, only_legit=True):
    ''' Create an instant DataFrame for the given timeplace from the documents
    in the database.
    '''
    
    filters = {'timeplace': timeplace}
    if collection:
        col = collection
    if only_legit:
        timeplace = timeplaces(col, only_legit=True)
    if return_list:
        return records_to_rows(col, filters, as_list=True)
    
    rdf = records_to_rows(col, filters)
    if drop_cols:
        rdf.drop(columns=drop_cols, inplace=True)
    # Change each tt_inst value to its nearest next multiple of 10800
    rdf['tt_inst'] = rdf.loc[:, 'tt_inst'].apply(
        pinky.favor, trans=False)
    rdf.set_index(['timeplace', 'tt_inst'], inplace=True)
    return rdf.sort_index(inplace=False)

def make_instants_from_df(df, _return=True):
    ''' Convert the rows of the weathers collection DataFrame to a DataFrame of
    instants.
    
    This is useful when you have a DataFrame already built from rows of raw 
    data representing individual documents as they came from the database and
    you want to create a DataFrame of flattened DataFrames
    '''
    
    d = []
    timeplaces = df.index.unique(level='timeplace')
    for tp in timeplaces:
        temp_df = df.loc[tp]
        d.append(flatten_to_single_row(temp_df))
    if _return:
        return pd.concat(d)
    else:
        np.save('instants.npy', pd.concat(d))
        return

### THIS HAS GOT TO BE MODIFIED TO BE USED WITHOUT DICT DATAFRAMES ###
def make_deltas(df):
    ''' Take a pandas.DataFrame and compare each of the items in the 'obs'
    row to each of those in the 'cast' rows. Return a pandas.DataFrame of
    comparison results.
    '''

#     deltas = []
    obs = df.loc[df['type'] == 'obs']#find_item_with_kv_pair(series, 'type', 'obs')
    cast = df.loc[df['type'] == 'cast']
    if obs != None:
        for item in cast:
            item[0] = item[0].subtract(obs, fill_value=np.nan)
    return cast#pd.Series(deltas, name=series.name, dtype=object)
### THIS HAS GOT TO BE MODIFIED TO BE USED WITHOUT DICT DATAFRAMES ###

def make_diff(from_list=None, from_df=None):
    ''' Get all the differences between the cast data and the obs data. '''

    if from_list:
        df_list = from_list
    for item in df_list:
        # Start by finding the DataFrame with 'obs' data.
        if 'obs' == item['type'].values[0]:
            dfs = []
            item['tt_inst'] = item.loc[:, 'tt_inst'].apply(pinky.favor, trans=False)
            # Loop through the list again, this time creating a differences frame.
            for df in df_list:
                if 'cast' == df['type'].values[0]:# and len(item['type'] == 1):
                    df['tt_inst'] = df.loc[:, 'tt_inst'].apply(pinky.favor, trans=False)
                    # Create a list of the intersection of the columns of the dfs.
                    on = []
                    for a in item.columns:
                        for b in df.columns:
                            if a in df.columns and b in item.columns:
                                on.append(a)

                    obs = item[set(on)].set_index('timeplace')
                    cast = df[set(on)].set_index('timeplace')

                    obs = obs.select_dtypes(exclude=['object'])
                    cast = cast.select_dtypes(exclude=['object'])
                    diff = cast.subtract(obs, axis = 1)#.set_index('timeplace')
                    dfs.append(diff)
    return pd.concat(dfs)

def do_diff_process(col):
    ''' Do the whole process of making diffs as it goes for making them
    direct from the database.
    
    :param col: The database collection to query.
    :type col: pymongo.collection.Collection
    '''
    
    diff_list = []
    
    # Get all the unique complete timeplaces in the database into a list.
    legits = timeplaces(col, only_legit=False)
    
    for tp in legits[:100]:
        print(tp)
        filters = {'timeplace': tp}
        df_list = records_to_rows(col, filters=filters, as_list=True)
        diff_list.append(flatten_to_single_row(make_diff(from_list=df_list)))
    return pd.concat(diff_list)


if __name__ == '__main__':
    diffs = do_diff_process(col)
    diffs = diffs.to_numpy()
    np.save('diffs.npy', diffs)
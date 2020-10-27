import time

import numpy as np
import pandas as pd
import benedict

import config
import pinky


# Setup the database connection and name the collection.
client = config.client
db = client[config.database]
col = db[config.weathers_collection]


def list_intersect(list1, list2):
    ''' Get the intersection of two lists. '''

    list2 = set(list2)
    return [value for value in list1 if value in list2]

def tups_to_dict(tups):
    ''' Convert a list of two-tuples to a dictionary. '''

    dicti = {}
    for a, b in tups:
        dicti.setdefault(a, b)
    return dicti

def timeplaces(col, only_legit=False, log=False):
    ''' Make a list of timeplaces from the collection.
    
    :param col: The database collection to query.
    :type col: pymongo.collection.Collection
    :param only_legit: Determines whether or not to return all timeplaces
    or only the legit ones.
    :type only_legit: bool
    '''
    
    could_be = []
    cannot_be = []
    not_legit = []
    legit = []
    
    raw = col.find({})
    timeplaces = [doc['timeplace'] for doc in raw if doc['type'] == 'obs']
    if log:
        for tp in timeplaces:
            _time = int(tp[-10:])
            now = int(time.time())
            if _time < now - 10800*41:
                if col.count_documents({'timeplace': tp}) == 41:
                    legit.append(tp)
                else:
                    not_legit.append(tp)
            else:
                cannot_be.append(tp)
        with open('legit.txt', 'w') as lt:
            for row in legit:
                lt.write(row+'\n')
        with open('working.txt', 'w') as w:
            for row in could_be:
                w.write(row+'\n')
    if only_legit:
        return legit
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

    df_copy = df.reset_index()
    index = []
    data = []
    # Create corrosponding lists of index names and data items that will be
    # used to create a dataframe.
    for row in df_copy.iterrows():
        for d, i in zip(row[1], row[1].index):
            index.append(str(i)+str(row[0]))
            data.append(d)
    d = pd.DataFrame(data, index=index)
    return d.transpose()

def make_instant_from_db(timeplace, collection=None, drop_cols=None, return_list=False, only_legit=True, for_process=False):
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
#     if for_process:
        
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

def make_diff(from_list=None, from_df=None, inst_list=None):
    ''' Get all the differences between the cast data and the obs data. '''

    if from_list:
        df_list = from_list
#     if not isinstance(inst_list, type(None)):
#         print('inst_list not None')
    for item in df_list:
        # Start by finding the DataFrame with 'obs' data.
        if 'obs' == item['type'].values[0]:
            dfs = []
            cast_dfs = []
            item['tt_inst'] = item.loc[:, 'tt_inst'].apply(pinky.favor, trans=False)
            # Loop through the list again, this time creating a differences frame.
            for df in df_list:
                if 'cast' == df['type'].values[0]:# and len(item['type'] == 1):
                    df['tt_inst'] = df.loc[:, 'tt_inst'].apply(pinky.favor, trans=False)
                    # Create a list of the intersection of the columns of the dfs.
                    on = list_intersect(item.columns, df.columns)
#                     for a in item.columns:
#                         for b in df.columns:
#                             if a in df.columns and b in item.columns:
#                                 on.append(a)

                    obs = item[on].set_index('timeplace')
#                     obs = item[set(on)].set_index('timeplace')
                    cast = df[on].set_index('timeplace')
#                     cast = df[set(on)].set_index('timeplace')

                    obs = obs.select_dtypes(exclude=['object'])
                    cast = cast.select_dtypes(exclude=['object'])
                    diff = cast.subtract(obs, axis=1)#.set_index('timeplace')
                    dfs.append(diff)
                    if isinstance(inst_list, list):  # Append this given param for use outside
                        cast_dfs.append(cast)  # this function.
    inst_list.append(pd.concat(cast_dfs))
    return pd.concat(dfs)

def do_diff_process(col, inst_list=None):
    ''' Do the whole process of making diffs as it goes for making them
    direct from the database.
    
    :param col: The database collection to query.
    :type col: pymongo.collection.Collection
    '''
    
    diff_list = []
    if isinstance(inst_list, list):
        inst_list = inst_list
    
    # Get all the unique complete timeplaces in the database into a list.
    legits = timeplaces(col, only_legit=True, log=True)
    
    for tp in legits[:]:
#         print(tp)
        filters = {'timeplace': tp}
        df_list = records_to_rows(col, filters=filters, as_list=True)
        diff_df = make_diff(from_list=df_list, inst_list=inst_list)
        diff_df = flatten_to_single_row(diff_df).set_index('timeplace0')
        diff_list.append(diff_df)
        
    return pd.concat(diff_list)


if __name__ == '__main__':
    inst_list = []

    diffss = do_diff_process(col, inst_list=inst_list)
    inst_list = [flatten_to_single_row(df).set_index('timeplace0') for df in inst_list]
    instantss = pd.concat(inst_list)
    
    if diffss.shape == instantss.shape:
        print('checks out!')
    else:
        print(diffss.shape, instantss.shape)
    diffs = diffss.to_numpy()
    instants = instantss.to_numpy()
    
    np.save('instants.npy', instants)
    np.save('diffs.npy', diffs)

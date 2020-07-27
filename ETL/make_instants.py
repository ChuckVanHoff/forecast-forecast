''' Make the instant documents. Pull all documents from the "forecasted" and
the "observed" database collections. Sort those documents according to the
type: forecasted documents get their forecast arrays sorted into forecast lists
within the documents having the same zipcode and instant values, observed
documents are inserted to the same document corrosponding to the zipcode and
instant values. '''


import time

import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

import db_ops
import config

# use the local host and port for all the primary operations
port = config.port #27017
host = config.host #'localhost'
client = config.client

def find_data(database, collection, filters={}):
    ''' Find the items in the specified database and collection using the filters.

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database
    name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a
    collection name present in the database
    :type collection: str
    :param filters: the parameters used for filtering the returned data. An
    empty filter parameter returns the full collection
    :type filters: dict
    
    :return: the result of the query
    :type: pymongo.cursor.CursorType
    '''

    col = db_ops.dbncol(client, database, collection)
    return col.find(filters).batch_size(100)

def update_command_for(data):
    ''' the 'update command' is the MongoDB command that is used to update data
    should be a weather type object. it will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the data dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    '''
    
    ### ORIGINAL ###
#     if data['_type'] == 'forecast':
#         filters = {'_id': data['timeplace']}
#         updates = {'$push': {'forecasts': data}} # append to forecasts list
#         return pymongo.UpdateOne(filters, updates,  upsert=True)
#     elif data['_type'] == 'observation':
#         filters = {'_id': data['timeplace']}
#         updates = {'$set': {'observation': data}}
#         return pymongo.UpdateOne(filters, updates,  upsert=True)
#     else:
#         filters = {'_id': 'update_command_for(data)error'}
#         updates = {'$set': {'errors': data}}
#         return pymongo.UpdateOne(filters, updates,  upsert=True)
    ### ORIGINAL ###
    
    ### wrap this in a try-except to get past KeyErrors. TEMP FIX FOR FINDING
    ### LEGITS IN OLD DATA
    try:
        if '_type' in data:
            if data['_type'] == 'forecast':
                updates = {'$push': {'forecasts': data}} # append to forecasts list
            elif data['_type'] == 'observation':
                updates = {'$set': {'observation': data}}
            if 'timeplace' in data:
                filters = {'_id': data['timeplace']}
            if '_id' in data:
                filters = {'_id': data['_id']}
            return pymongo.UpdateOne(filters, updates,  upsert=True)
        elif 'forecasts' in data or 'observations' in data or 'observation' in data:

            ### change the key name from observaTION to observaTIONS ###
            if 'observation' in data:
                data['observations'] = data.pop('observation')
            ### change the key name from observaTION to observaTIONS ###

            ### I am not sure if there will be '_id' or 'timeplace' in the data
            ### coming in from the function argument.
#             filters = {'_id': data['_id']}
                try:
                    filters = {'_id': data['_id']}
                except KeyError as e:
                    print(f'KeyError: {e}. Attempt setting filter using timeplace')
                    filters = {'_id': data['timeplace']}
            ### I am not sure if there will be '_id' or 'timeplace' in the data
            ### coming in from the function argument.

            return pymongo.InsertOne(data)
    except KeyError as e:
        print(f'Had a KeyError for {e} while attempting update_command_for(). \
        In case you are curious, these are the keys that are in the data: \
        {data.keys()}')
        updates = {'$set': {'errors': data}}
        filters = {'_id': 'update_command_for(data)error'}
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    ### wrap this in a try-except to get past KeyErrors. TEMP FIX FOR FINDING
    ### LEGITS IN OLD DATA

def make_load_list_from_cursor(cursor):
    ''' create the list of objects from the database to be loaded through
    bulk_write()
    
    :param cursor: it is just what the name says it is
    :type cursor: a pymongo cursor
    :return update_list: list of update commands for the weather objects on the
    cursor
    '''

    update_list = []
    
    try:
        n=0
        for obj in cursor:
            update_list.append(update_command_for(obj))
            n+=1
        return update_list
    except:
        print('Error making load_list')
        return load_list[:n]

def make_instants(client,
                  cast_col=config.forecast_collection,
                  obs_col=config.observation_collection,
                  inst_col=config.instants_collection
                 ):
    ''' Make the instant documents, as many as you can, with the data in the
    named database.
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives.
    
    :param client: a MongoDB client
    :type client: pymongo.MongoClient
    '''
    
    #Get the data
    forecasts = db_ops.dbncol(client, cast_col, config.database)
    observations = db_ops.dbncol(client, obs_col, config.database)
    instants = db_ops.dbncol(client, inst_col, config.database)

    
    ### Added the function sub_make() to help break up the really big cursors
    cast_count = forecasts.count_documents({})
    obs_count = observations.count_documents({})
    print(cast_count)
    print(obs_count)
    n = 0
    m = 0
    
    def sub_make(sub_col):
        ''' sub_col is some cursor on the collection'''
#         print('in sub_make()')
        instants.create_index([('_id', pymongo.ASCENDING)])
        # make the load lists and load the data
        load_list = make_load_list_from_cursor(sub_col)
        if len(load_list) == 0:
            return -1
        inserted = instants.bulk_write(load_list).upserted_ids

        # Delete the used documents.
        update_list = []
        for _id in inserted.values():
            _id = {'_id': _id}
            update_list.append(pymongo.operations.DeleteOne(_id))
        if update_list:
            return update_list
        else:
            return -1

    while n+100 < cast_count:
#         print('processing casts')
#         print(n)
        casts = forecasts.find({})[n:n+100]
        inserts = sub_make(casts)
        n += 100
#         print(n)
#         forecasts = db_ops.dbncol(client, cast_col, config.database)
        if inserts == -1:
            continue
        else:
            forecasts.bulk_write(inserts)
    while m+100 < obs_count:
#         print('processing obs')
#         inst_col.create_index([('timeplace', pymongo.DESCENDING)])
        obs = observations.find({})[m:m+100]
        inserts = sub_make(obs)
        m += 100
#         observations = db_ops.dbncol(client, obs_col, config.database)
        if inserts == -1:
            continue
        else:
            observations.bulk_write(inserts)

#     print('past the sub_make() loops and on to the final stretch')
    sub_make(forecasts.find({})[:n])
    sub_make(observations.find({})[:n])
    ### Added the function sub_make() to help break up the really big cursors
#     print('Finished')
    return

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
client = MongoClient(host, port)

def find_data(client, database, collection, filters={}):
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

#     db = Database(client, database)
#     col = Collection(db, collection)
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
    
    
    if data['_type'] == 'forecast':
        filters = {'_id': data['timeplace']}
        updates = {'$push': {'forecasts': data}} # append to forecasts list
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    elif data['_type'] == 'observation':
        filters = {'_id': data['timeplace']}
        updates = {'$set': {'observation': data}}
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    else:
        filters = {'_id': 'update_command_for(data)error'}
        updates = {'$set': {'errors': data}}
        return pymongo.UpdateOne(filters, updates,  upsert=True)

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

def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
    ''' Move or copy a collection within and between databases. 
    
    :param col: the collection to be copied
    :type col: a pymongo collection or 
    :param destination_col: the collection you want the documents copied into
    :type destination_col: a pymongo.collection.Collection object
    :param destination_db: the database with the collection you want the
    documents copied into
    :type destination_db: a pymongo database pymongo.databse.Database
    :param filters: a filter for the documents to be copied from the collection.
    :type filters: dict
    :param delete: Determine if the orignials should be deleted. Default is no.
    :type delete: bool
    '''

    copy = []
    for item in col.find(filters):
        copy.append(item)
    destination = db_ops.dbncol(client, destination_col, destination_db)    
    try:
        inserted_ids = destination.insert_many(copy).inserted_ids
        if delete == True:
            # remove all the documents from the original collection
            for row in inserted_ids:
                filters = {'_id': row}
                col.delete_one(filters)
    except pymongo.errors.BulkWriteError as e:
        print(f'The documents have not been copied to {destination_col}.')
        print(e)
    return

def make_instants(client, cast_col, obs_col, inst_col):
    ''' Make the instant documents, as many as you can, with the data in the
    named database. 
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives.
    
    :param client: a MongoDB client
    :type client: pymongo.MongoClient
    '''
    
    #Get the data
    cast_col = db_ops.dbncol(client, cast_col, config.database)
    obs_col = db_ops.dbncol(client, obs_col, config.database)
    inst_col = db_ops.dbncol(client, inst_col, config.database)
    forecasts = cast_col.find({})
    observations = obs_col.find({})
    
    inst_col.create_index([('timeplace', pymongo.DESCENDING)])
    
    # make the load lists and load the data
    cast_load_list = make_load_list_from_cursor(forecasts)
    obs_load_list = make_load_list_from_cursor(observations)
    cast_inserted = inst_col.bulk_write(cast_load_list).upserted_ids
    obs_inserted = inst_col.bulk_write(obs_load_list).upserted_ids

#    # Copy the docs to archive storage and delete the source data.
#    copy_docs(cast_col, config.database, 'cast_archive', delete=True)
#    copy_docs(obs_col, config.database, 'obs_archive', delete=True)

    # Delete the used documents. The data is all contained in the insants, so
    # there's no reason to keep it around taking up space.
    cast_update_list = []
    obs_update_list = []
    for c_id in cast_inserted.values():
        c_id = {'_id': c_id}
        cast_update_list.append(pymongo.operations.DeleteOne(c_id))
    for o_id in obs_inserted.values():
        o_id = {'_id': o_id}
        obs_update_list.append(pymongo.operations.DeleteOne(o_id))
    if cast_update_list:
        cast_col.bulk_write(cast_update_list)
    if obs_update_list:
        obs_col.bulk_write(obs_update_list)
    return

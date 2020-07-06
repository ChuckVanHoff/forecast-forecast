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

    client = MongoClient(host, port)
    db = Database(client, database)
    col = Collection(db, collection)
    data = col.find(filters).batch_size(100)
    client.close()
    return data

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
        filters = {'timeplace': data['timeplace']}
        updates = {'$push': {'forecasts': data}} # append to forecasts list
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    elif data['_type'] == 'observation':
        filters = {'timeplace': data['timeplace']}
        updates = {'$set': {'observations': data}}
        return pymongo.UpdateOne(filters, updates,  upsert=True)
    else:
        filters = {'timeplace': data['timeplace']}
        updates = {'$set': data}
        print('made load command for an instant')
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
        return update_list
    except:
        print('Error making load_list')
    return

def make_instants():
    ''' Make the instant documents, as many as you can, with the data in the
    named database.
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives.
    
    :param client: a MongoDB client
    :type client: pymongo.MongoClient
    '''

    client = MongoClient(host, port)
    # Get the data.
    cast_col = db_ops.dbncol(client, "cast_temp", config.database)
    obs_col = db_ops.dbncol(client, "obs_temp", config.database)
    inst_col = db_ops.dbncol(client, "instant_temp", config.database)
    forecasts = cast_col.find({}).batch_size(100)
    observations = obs_col.find({}).batch_size(100)
    
    inst_col.create_index([('timeplace', pymongo.DESCENDING)])
    
    # make the load lists and load the data
    cast_load_list = make_load_list_from_cursor(forecasts)
    obs_load_list = make_load_list_from_cursor(observations)
    inst_col.bulk_write(cast_load_list)
    inst_col.bulk_write(obs_load_list)
    
    # Copy the docs to archive storage and delete the source data.
    # db_ops.copy_docs(cast_col, config.database, 'cast_archive', delete=True)
    # db_ops.copy_docs(obs_col, config.database, 'obs_archive', delete=True)

    client.close()
    return

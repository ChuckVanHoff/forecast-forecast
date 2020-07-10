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
    ''' The 'update command' is the MongoDB command that is used to update data
    should be a weather type object. It will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the data dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    
    try:
		if data['_type'] == 'forecast':
			filters = {'_id': data['timeplace']}
			updates = {'$push': {'forecasts': data}} # append to forecasts list
			return pymongo.UpdateOne(filters, updates,  upsert=True)
		elif data['_type'] == 'observation':
			filters = {'_id': data['timeplace']}
			updates = {'$set': {'observation': data}}
			return pymongo.UpdateOne(filters, updates,  upsert=True)
		else:
			### I am betting that you'll be back here make this stop happening, or
			### just handle the situation where there's no forecast or observation.
			filters = {'_id': 'update_command_for(data)error'}
			updates = {'$set': {'errors': data}}
	except KeyError as e:
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
            n+=1
        return update_list
    except:
        print('Error making load_list')
        return load_list[:n]

def make_instants(client, cast_col, obs_col, inst_col):
    ''' Make the instant documents, as many as you can, with the data in the
    named database.
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives or delete
	them (switch using a comment/un-comment of the copy and delete blocks below).
    
    :param client: a MongoDB client
    :type client: pymongo.MongoClient
	:param cast_col: The collection containing the forecast data you want to use
	:type cast_col: pymongo.collection.Collection
	:param obs_col: The collection containing the observation data you want to use
	:type obs_col: pymongo.collection.Collection
	:param inst_col: The collection containing the instant data you want to use
	:type inst_col: pymongo.collection.Collection
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

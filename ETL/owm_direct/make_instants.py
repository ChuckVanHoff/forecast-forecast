''' Make the instant documents. Pull all documents from the "forecasted" and
the "observed" database collections. Sort those documents according to the
type: forecasted documents get their forecast arrays sorted into forecast lists
within the documents having the same zipcode and instant values, observed
documents are inserted to the same document corrosponding to the zipcode and
instant values. '''


import time

import pymongo

import db_ops
import config


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
    
    tti = {str(data['tt_inst']): data}  # Take the tt_inst from data and set it
                                    # up as the key for the data set.
    updates = {'$set': tti} 
    filters = {'_id': data['timeplace']}  # Add the data to the object with
                                    # a matching timeplace
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
    
    for obj in cursor:
        update_list.append(update_command_for(obj))
    if len(update_list) == 0:
        print('update_list is empty')
    return update_list
#     try:
#         n=0
#         for obj in cursor:
#             update_list.append(update_command_for(obj))
#             n+=1
#         return update_list
#     except:
#         print('Error making load_list')
#         return update_list[:n]

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

def make_instants():
    ''' Make the instant documents, as many as you can, with the data in the
    named database.
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives.
    '''
    
    #Get the data
    weather_col = db_ops.dbncol(
        config.client,
        config.weathers_collection,
        config.database
    )
    inst_col = db_ops.dbncol(
        config.client,
        config.instants_collection,
        config.database
    )
    weathers = weather_col.find({}).batch_size(100)
    inst_col.create_index('timeplace')
    
    # make the load lists and load the data
    weathers_load_list = make_load_list_from_cursor(weathers)
    weathers_inserted = inst_col.bulk_write(weathers_load_list).upserted_ids

    # Delete the used documents.
    weathers_update_list = []
    for w_id in weathers_inserted.values():
        w_id = {'_id': w_id}
        weathers_update_list.append(pymongo.operations.DeleteOne(w_id))
    if weathers_update_list:
        weather_col.bulk_write(weathers_update_list, ordered=False)
    return

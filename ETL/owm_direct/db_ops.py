''' Useful functions for forecast-forecast specific operations '''

import time
import collections

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.errors import InvalidDocument, OperationFailure
from pymongo.errors import BulkWriteError, ConfigurationError

import config

host = config.host
port = config.port
uri = config.uri

def check_db_access(client):
    ''' A check that there is write access to the database. '''
    
###
### Add some stuff that will check the internet connection, the status of
### the mongod instance, etc when there is not db access.
###

    db = client.test_db
    col = db.test_col
    db_count_pre = 0
    db_count_poost = 0

    
    # Check on this particular client's write status.
    try:
        if client.admin.command('ismaster'):
            print('You have write access!')
        else:
            print('According to the docs, this command came from a secondary \
            member or an arbiter of a replica set. You have no write access.')
    except ConnectionFailure:
        print("Server not available")
        return False
    return True

def Client(uri):
    ''' Create and return a pymongo MongoClient object. If the uri is given but
    for whatever reason the MongoClient cannot be made, then revert to the local
    instance of a MongoDB server.
    
    *** This function is most appropriately used for the remote client
    connection using a proper uri; for local connections you should just use the
    pymongo MongoClient() as is.
    ***

    :param uri: the remote server URI. must be uri encoded
    type uri: uri encoded sting
    '''
    
    if uri:
        try:
            client = MongoClient(uri)
        except:
            # Regardless of the error, print the error message and connect to the
            # local MongoDB instance.
            print('There was a problem connecting with the uri...going local.')
            client = MongoClient()
        return client
    else:
        try:
            client = MongoClient()
            return client
        except ConnectionFailure:
            print('caught ConnectionFailure on local server. Returning -1 flag')
            return -1
    
def dbncol(client, collection, database):
    ''' Make a connection to the database and collection given in the arguments.

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database
    name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a
    collection name present in the database
    :type collection: str
    
    :return col: the collection to be used
    :type: pymongo.collection.Collection
    '''

    try:
        db = Database(client, database)
    except AttributeError as e:
        print(f'dbncol caught AttributeError while trying to connect {client}.')
        print(e, '...trying to connect with the remote, if I can.')
        from config import uri
        client = MongoClient(uri)
        db = Database(client, database)
        print('did it without issue.')
    col = Collection(db, collection)
    return col

def read_mongo_to_dict(collection, query={}, limit=None):
    ''' Read the colleciton to a dictionary.

    :param collection: MongoDB collection
    :type collection: pymongo.collection.Collection
    :param query: the collection query
    :type query: a mongodb appropriate, dictionary-style dict
    :param limit: a value that will limit the returned documents read to dict
    :type limit: int It must be less than or equal to the number of docs on the
    returned cursor.
    '''

    if limit:
        cursor = collection.find(query).batch_size(100)[:limit]
    else:
        cursor = collection.find(query).batch_size(100)
    return {curs.pop('_id'): curs for curs in cursor}

def load(data, database, collection, client=config.client):
    ''' Load data to specified database collection. Also checks for a
    preexisting document with the same instant and zipcode, and updates it in
    the case that there was already one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    '''

    col = dbncol(client, collection, database)

    # set the appropriate database collections, filters and update types
    if collection == 'instant':
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$push': {'forecasts': data}} # append to the forecasts list
        try:
            # check to see if there is a document that fits the parameters. If
            # there is, update it, if there isn't, upsert it.
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            client.close()
            return(f'DuplicateKeyError, could not insert data to {collection}')
    elif collection == 'observed' \
        or collection == 'forecasted' \
        or collection == 'obs_temp' \
        or collection == 'cast_temp' \
        or collection == config.observation_collection \
        or collection == config.forecast_collection \
        or collection == config.instants_collection:
        try:
            col.insert_one(data)
            client.close()
            return
        except DuplicateKeyError:
            client.close()
            return(f'DuplicateKeyError, could not insert data to {collection}')
    else:
        try:
            filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
            updates = {'$set': {'forecasts': data}} # append to forecasts list
            client.close()
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            client.close()
            return(f'DuplicateKeyError, could not insert data to {collection}')

def copy_docs(col, destination_client, destination_db, destination_col, filters={}, delete=False):
    ''' Move or copy a collection within and between databases.
    
    :param col: The collection to be copied from.
    :type col: pymongo.collection.Collection
    :param destination_col: The name of the collection you want the documents
        copied into.
    :type destination_col: string
    :param destination_db: The name of the database you want the documents
        copied into.
    :type destination_db: string
    :param filters: a filter for the documents to be copied from the collection
    By default all collection docs will be copied
    :type filters: dict
    '''
    
    n = 0
    ids = []  # For holding filters for the delete function.
    count = col.count_documents(filters)  # For keeping list slices in range.
    # Define the database and collections.
    client = destination_client
    database = destination_db
    collection = destination_col
    destination = dbncol(client, collection, database)

    ### Attempting to wrap this in a while-loop to break it up and save my RAM
    ### and swap memories.
    while n <= count:
        # Since updating the source with each pass through the loop, there is 
        # also a need to perform a new query each pass.
        original = col.find(filters)
        temp = []
        ids = []
        
        # Take the next 100 documents or take all the way to the end.
        if n+100 <= count:
            for item in original[n:n+100]:
                temp.append(item)
            n += 100
        else:
            for item in original[n:]:
                temp.append(item)
            n = count + 1  # Incriment this to stop the loop.

        # Now do everything you need to do to copy the set of documents and
        # delete the originals when commanded.
        if not temp:
            break
        try:
            inserted_ids = destination.insert_many(
                temp, 
                bypass_document_validation=True,
                ordered=False
            ).inserted_ids
        except BulkWriteError as bwe:
            inserted_ids = []
        if delete == True and inserted_ids:

            # Enter ids into a dictionary for the delete function and delete.
            for item in inserted_ids:
                filt = {'_id': item}
                ids.append(filt)
            try:
                for f in ids:
                    result = col.delete_many(f)
            except BulkWriteError as bwe:
                with open('bwe_log.txt', 'a') as log:
                    log.write(time.ctime())
                    log.write(str(bwe.details))
        original = col.find(filters)  # Do a new 
    return

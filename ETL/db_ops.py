''' Useful functions for forecast-forecast specific operations '''

import time

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.errors import InvalidDocument, OperationFailure, ConfigurationError

import config

host = config.host
port = config.port
uri = config.uri

def check_db_access():
    '''A check that there is write access to the database'''
 
    client = MongoClient(host, port)
    
    try:
        client.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available")
    # check the database connections
        # Get a count of the databases in the beginning
        # Add a database and collection
        # Insert something to the db
        # Get a count of the databases after adding one
    db_count_pre = len(client.list_database_names())
    db = client.test_db
    col = db.test_col
    post = {'name':'Chuck VanHoff',
           'age':'38',
           'hobby':'gardening'
           }
    col.insert_one(post)
    db_count_post = len(client.list_database_names())
    if db_count_pre - db_count_post >= 0:
        print('Your conneciton is flipped up')
    else:
        print('You have write access')
    # Dump the extra garbage and close out
    client.drop_database(db)
    client.close()
    return

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
            return client
        except:
            # Regardless of the error, print the error message and connect to the
            # local MongoDB instance.
            host = 'localhost'
            port = 27017
            client = MongoClient(host=host, port=port)
            return client
    else:
        try:
            client = MongoClient(host=host, port=port)
            return client
        except ConnectionFailure:
            print('caught ConnectionFailure on local server. Returning -1 flag')
            return -1
    
def dbncol(client, collection, database): ###=database): I don't think this is needed anymore
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

def load(data, database, collection):
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

    client = MongoClient(host, port)
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
        or collection == 'cast_temp':
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

def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
    ''' Move or copy a collection within and between databases.
    
    :param col: the collection to be copied
    :type col: a pymongo collection
    :param destination_col: the collection you want the documents copied into
    :type destination_col: a pymongo.collection.Collection object
    :param destination_db: the database you want the documents copied into
    :type destination_db: a pymongo database pymongo.database.Database
    :param filters: a filter for the documents to be copied from the collection
    By default all collection docs will be copied
    :type filters: dict
    '''
	
	n = 0
	count = col.count_documents()
    original = col.find(filters).batch_size(100)
    copy = []
	### Attempting to wrap this in a while-loop to break it up and save my RAM
	### and swap memories.
	while original.is_alive():
		if n+100 <= count:
			### DO YOU REALLY NEED TO PUT THESE INTO A COPY, OR CAN YOU INSERT
			### THEM RIGHT OFF THE CURSOR?
			for item in original[n:n+100]:
				copy.append(item)
			n += 100
        	# Now do everything you need to do to copy the set of documents.
			database = destination_db     # Define database and collection
			collection = destination_col  # for the following operations.
			destination = dbncol(client, collection, database)
			inserted_ids = destination.insert_many(copy).inserted_ids
			if delete == True:
				# remove all the inserted documents from the origin collection.
				for item in inserted_ids.values():
					filter = {'_id': item}
					col.delete_one(filter)
			### Commented  because I'm waiting for the process to complete
			### before printing a message to the screen
# 				print(f'MOVED 100 docs from {col} to {destination}.')
# 			else:
# 				print(f'COPIED docs in {col} to {destination}.')
		else:
			for item in original[n:]:
				copy.append(item)
			# Now do everything you need to do to copy the set of documents.
			database = destination_db     # Define database and collection
			collection = destination_col  # for the following operations.
			destination = dbncol(client, collection, database)
			inserted_ids = destination.insert_many(copy).inserted_ids
			if delete == True:
				# remove all the inserted documents from the origin collection.
				for item in inserted_ids.values():
					filter = {'_id': item}
# 					col.delete_one(filter)
# 				print(f'MOVED 100 docs from {col} to {destination}.')
# 			else:
# 				print(f'COPIED docs in {col} to {destination}.')
	if delete == True:
		print(f'MOVED docs from {col} to {destination}.')
	else:
		print(f'COPIED docs in {col} to {destination}.')
    return

    
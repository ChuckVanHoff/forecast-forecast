''' Make the instant documents. Pull all documents from the "forecasted" and
the "observed" database collections. Sort those documents according to the
type: forecasted documents get their forecast arrays sorted into forecast lists
within the documents having the same zipcode and instant values, observed
documents are inserted to the same document corrosponding to the zipcode and
instant values. '''


import time

import pymongo
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import DuplicateKeyError, ConnectionFailure
from pymongo.errors import InvalidDocument, OperationFailure, ConfigurationError
# from urllib.parse import quote

import db_ops
import config

# use the local host and port for all the primary operations
port = config.port #27017
host = config.host #'localhost'
client = MongoClient(host=host, port=port)

### THIS FUNCTION IS IN THE MODULE db_ops ###
# def Client(host=None, port=None, uri=None):
#     ''' Create and return a pymongo MongoClient object. Connect with the given
#     parameters if possible, switch to local if the remote connection is not
#     possible, using the default host and port.
    
#     :param host: the local host to be used. defaults within to localhost
#     :type host: sting
#     :param port: the local port to be used. defaults within to 27017
#     :type port: int
#     :param uri: the remote server URI. must be uri encoded
#     type uri: uri encoded sting'''
    
#     if host and port:
#         try:
#             client = MongoClient(host=host, port=port)
#             return client
#         except ConnectionFailure:
#             # connect to the remote server if a valid uri is given
#             if uri:
#                 print('ConnectionFailure on local. Trying it with remote')
#                 client = MongoClient(uri)
#                 print(f'established remote MongoClient on URI={uri}')
#                 return client
#             print('caught ConnectionFailure on local server. Returning None')
#             return None
#     elif uri:
#         # verify that the connection with the remote server is active and
#         # switch to the local server if it's not
#         try:
#             client = MongoClient(uri)
#             return client
#         except ConfigurationError:
#             print(f'Caught configurationError in client() for URI={uri}.')
#             client = MongoClient(host=host, port=port)
#             print('connection made with local server; you asked for remote.')
#             return client

### THIS FUNCTION IS IN THE MODULE db_ops ###
# def dbncol(client, collection, database):
#     ''' Make a connection to the database and collection given in the arguments.

#     :param client: a MongoClient instance
#     :type client: pymongo.MongoClient
#     :param database: the name of the database to be used. It must be a database
#     name present at the client
#     :type database: str
#     :param collection: the database collection to be used.  It must be a
#     collection name present in the database
#     :type collection: str
    
#     :return col: the collection to be used
#     :type: pymongo.collection.Collection
#     '''

#     db = Database(client, database)
#     col = Collection(db, collection)
#     return col

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

    db = Database(client, database)
    col = Collection(db, collection)
    return col.find(filters).batch_size(100)

def load_weather(data, client, database, collection):
    ''' Load data to specified database collection. This determines the
    appropriate way to process the load depending on the collection to which
    it should be loaded. Data is expected to be a weather-type dictionary. When
    the collection is "instants" the data is appended the specified object's
    forecasts array in the instants collection; when the collection is either
    "forecasted" or "observed" the object is insterted uniquely to the
    specified collection. Also checks for a preexisting document with the same
    instant and zipcode, then updates it in the case that there was already
    one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    ''' 
    col = db_op.dbncol(client, collection, database=database)
    # decide how to handle the loading process depending on where the document
    # will be loaded.
    if collection == 'instant' or collection == 'test_instants':
        # set the appropriate database collections, filters and update types
        if "Weather" in data:
            updates = {'$set': {'weather': data['Weather']}}
        else:
            updates = {'$push': {'forecasts': data}} # append to forecasts list
        try:
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
        except KeyError:
            print('you just got keyerror on something')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
        
def update_command_for(data):
    ''' the 'update command' is the MongoDB command that is used to update data
    should be a weather type object. it will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    
    from pymongo import UpdateOne
    
    # Logic for the weather.Weather class days
    if data['_type'] == 'forecast':
        filters = {'_id': data['_id']}
        updates = {'$push': {'forecasts': data}} # append to forecasts list
        return UpdateOne(filters, updates,  upsert=True)
    elif data['_type'] == 'observation':
        filters = {'_id': data['_id']}
        updates = {'$set': {'observations': data}}
        return UpdateOne(filters, updates,  upsert=True)
    # Logic for the pre-weather.Weather class days

    elif "Weather" in data:
        try:
            filters = {'zipcode': data['Weather'].pop('zipcode'),
                        'instant': data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except:
            ### for processing data in OWM.forecasted and OWM.observed.
            if "Weather" in data:
                try:
                    data['Weather']['time_to_instant'] = \
                            data['Weather'].pop('reference_time')\
                            - data['reception_time']
                    filters = {'zipcode': data.pop('zipcode'),\
                                'instant': data.pop('instant')}
                    updates = {'$set': {'weather': data['Weather']}}
                except KeyError:
                    print('caught KeyError')
            else:
                try:
                    filters = {'zipcode': data.pop('zipcode'),\
                                'instant': data.pop('instant')}
                    updates = {'$push': {'forecasts': data}} 
                except KeyError:
                    print('caught keyerror')
    else:
        try:
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
            return UpdateOne(filters, updates,  upsert=True)
        except KeyError as e:
            if e.args == 'zipcode' or e.args == 'instant':
                print(f'It was a keyerror on {e.args}. Here is the data:', data)
            else:
                # Print the error and try to make an update command for it to
                # be added to the updates list.
                print(e)

def delete_command_for(data):
    ''' the 'delete command' is the MongoDB command that is used to update data
    should be a weather type object. it will have its filter and update set
    according to the entry content. It returns a command to update in a pymongo
    database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    from pymongo import DeleteOne

    # catch the error if this is processing data entered by another module or
    # version of this one, but otherwise expect there to be ..... come back
    if "Weather" in data:
        try:
            filters = {'zipcode': data['Weather'].pop('zipcode'),\
                        'instant': data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except KeyError:
            # this if for processing data in OWM.forecasted and OWM.observed.
            data['Weather']['time_to_instant'] \
                    = data['Weather'].pop('reference_time')\
                    - data['reception_time']
            filters = {'zipcode': data.pop('zipcode'),\
                        'instant': data.pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except KeyError:
            print('caught KeyError')
    else:
        try:
            filters = {'zipcode': data.pop('zipcode'),\
                        'instant': data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
        except KeyError:
            print('caught keyerror')
    return DeleteOne(filters, updates,  upsert=True)


def make_load_list_from_cursor(pymongoCursorOnWeather):
    ''' create the list of objects from the database to be loaded through
    bulk_write()
    
    :param pymongoCursorOnWeather: it is just what the name says it is
    :type pymongoCursorOnWeather: a pymongo cursor
    :return update_list: list of update commands for the weather objects on the
    cursor
    '''

    update_list = []
    
    ### Logic for the weather.Weather class days ###
    try:
        n=0
        for obj in pymongoCursorOnWeather:
            update_list.append(update_command_for(obj))
        return update_list
    except:
        print('Error making load_list')
        exit()
    ### Logic for the weather.Weather class days ###
        
    ### Logic for the pre-weather.Weather class  days ###    
    # check the first entry to know whether it's forecast or observation
    if 'Weather' in pymongoCursorOnWeather[0]:
        for obj in pymongoCursorOnWeather:
            update_list.append(update_command_for(obj))
        return update_list
    else:
        print('Did not find weather or Weather in pymongoCursor')
        for obj in pymongoCursorOnWeather:
            # Trying things that will capture any of the formats published over
            # the developemnet period.
            try:
                if 'reception_time':
                    update_list.append(update_command_for(cast))
                    print('found reception_time')
                    casts = obj['weathers'] # use the 'weathers' array from the
                                            # forecast
                    for cast in casts:
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant']\
                                                - obj['reception_time']
                        update_list.append(update_command_for(cast))
                    return update_list
                else:
                    casts = obj['weathers'] # use the 'weathers' array from the
                                            # forecast
                    for cast in casts:
                        # this is just setting the fields as I need them for
                        # each update object as it gets loaded to the database
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant']\
                                                - cast['reception_time']
                        update_list.append(update_command_for(cast))
                    return update_list
            except KeyError as e:
                print(f'from make_instants.make_load_list_from_cursor(): KeyError.args = {e.args}')
                print(e.with_traceback)
                return

def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
    ''' move or copy a collection within and between databases 
    
    :param col: the collection to be copied
    :type col: a pymongo collection or 
    :param destination_col: the collection you want the documents copied into
    :type destination_col: a pymongo.collection.Collection object
    :param destination_db: the database with the collection you want the
    documents copied into
    :type destination_db: a pymongo database pymongo.databse.Database
    :param filters: a filter for the documents to be copied from the collection.
    By default all collection docs will be copied
    :type filters: dict
    '''

#     client = MongoClient(host=host, port=port)
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

def make_instants(client):
    ''' Make the instant documents, as many as you can, with the data in the
    named database. 
    
    - Process: Connect to the database, get the data from the database, make a
    list lf load commands to get each document sorted into its proper instant
    document, and finally move the sorted documents to their archives.
    
    :param client: a MongoDB client
    :type client: pymongo.MongoClient
    '''
    
    #Get the data
    cast_col = db_ops.dbncol(client, "cast_temp", config.database)
    obs_col = db_ops.dbncol(client, "obs_temp", config.database)
    inst_col = db_ops.dbncol(client, "instant_temp", config.database)
    forecasts = cast_col.find({})
    observations = obs_col.find({})
    
    inst_col.create_index([('timeplace', pymongo.DESCENDING)])
    
    # make the load lists and load the data
    cast_load_list = make_load_list_from_cursor(forecasts)
    obs_load_list = make_load_list_from_cursor(observations)
    inst_col.bulk_write(cast_load_list)
    inst_col.bulk_write(obs_load_list)
    
    # Copy the docs to archive storage and delete the source data.
    copy_docs(cast_col, config.database, 'cast_archive', delete=True)
    copy_docs(obs_col, config.database, 'obs_archive', delete=True)
    return

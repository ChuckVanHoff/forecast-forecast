
def load_weather(data, client, database, collection):
    ''' Load data to specified database collection. This determines the
    appropriate way to process the load depending on the collection to which it
    should be loaded. Data is expected to be a weather-type dictionary. When
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
    col = db_ops.dbncol(client, collection, database=database)
    # decide how to handle the loading process depending on where the document
    # will be loaded.
    if collection == 'instant' or collection == 'test_instants' or collection == 'instant_temp':
        
        # set the appropriate database collections, filters and update types
        if "Weather" in data:
            filters = {'zipcode':data['Weather'].pop('zipcode'),
                        'instant':data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        else:
            filters = {'zipcode':data.pop('zipcode'),
                        'instant':data.pop('instant')}
            updates = {'$push': {'forecasts': data}} # append to forecasts list
        
        # Now attempt to load the data using the filters and updates.
        try:
            col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    
    elif collection == 'observed'\
        or collection == 'forecasted'\
        or collection == 'obs_temp'\
        or collection == 'cast_temp':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')

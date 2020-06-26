def load_og(data, client, database, collection):
    # Legacy function...see load_weather() for loading needs
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
    # create the filters and update types using the data in the dictionary
    if collection == 'instant':
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$push': {'forecasts': data}}    # append the forecast object
                                                    # to the forecasts list
        try:
            # check to see if there is a document that fits the parameters.
            # If there is, update it, if there isn't, upsert it
            col.find_one_and_update(filters, updates, upsert=True)
#             col.find_one_and_update(filters, updates,  upsert=True)
            return
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')


''' Defines the Instant class and some useful functions. When executed as main
this module will search the database for completed and incompletable Instants
and move the completed to a remote database-collection, legit_inst, and delete
all the incompletable instants.'''


class Instant:
    ''' The Instant is an object built out of a collection of Weathers: in the
    case of forecast-forecast there are 40 forecast dictionaries contained in a
    list and one observation dictionary for comparison.
    '''

    def __init__(self, timeplace, forecasts=[], observations={}):
        
        self.timeplace = timeplace
        self.casts = forecasts
        self.obs = observations
        self.as_dict = {'timeplace': self.timeplace,
                        'forecasts': self.casts,
                        'observations': self.obs
                        }
    
    @property
    def count(self):
        ''' Get the count of the elements in self.casts. '''
        
        return len(self.casts)
    
    @property
    def itslegit(self):
        ''' Check the instant's weathers array's count and if it is 40, then
        True is returned, otherwise it returns False.

        :param instant: the instant docuemnt to be legitimized
        :type instant: dictionary
        :return: Boolian value (True or False)
        '''
        
        if self.count == 40:
            return True
        else:
            return False
    
    def to_dbncol(self, collection='test'):
        ''' Load the data to the database. 

        :param collection: the collection name
        :type collection: string
        '''

        import config
        import db_ops
        
        col = db_ops.dbncol(client, collection, config.database)
        
        if self.itslegit:
            col.update_one({'timeplace': self.timeplace},
                           {'$set': self.as_dict},
                           upsert=True
                          )
        return


def convert(instants):
    ''' Convert a list of instants to instant.Instant objects.
    
    :param instants: the instants
    :type instants: This function is able to handle multiple types-
        dict: must be a dict of dicts and sets the instant timeplace to
            whatever the primary keys are.
        pymongo.collection.Collection: the collection will be converted to a
            cursor. It must have the same data fields that the dictionary must.
        pymongo.cursor.CursorType: A cursor over a collection query result.
            It has to have the same data that the dict has to have.
            
    :return: dict of instant.Instant objects
    '''
    
    import pymongo
    from pymongo.cursor import CursorType
    
    converted = {}
    try:
        if isinstance(instants, dict):
            # print('instant.convert() working on a dict')
            for key, value in instants.items():
                if 'observation' in value:
                    converted[key] = Instant(
                        key,
                        value['forecasts'],
                        value['observation']
                    )
                elif 'observations' in value:
                    converted[key] = Instant(
                        key,
                        value['forecasts'],
                        value['observations']
                    )
                else:
                    converted[key] = Instant(
                        key,
                        value['forecasts']
                    )
            return converted
        elif isinstance(instants, pymongo.collection.Collection):
            # print('instant.convert() working on a pymongo.collection.Collection')
            for doc in instants.find({}).batch_size(100):
                if 'observation' in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts'],
                            doc['observation']
                        )
                elif  'observations' in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts'],
                            doc['observation']
                        )
                elif 'observations' not in doc and 'observation' not in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts']
                        )
                else:
                    continue
            return converted
        elif type(instants) == pymongo.cursor.Cursor:
            # print('instant.convert() working on a pymongo.cursor.Cursor')
            for doc in instants:
                if 'observation' in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts'],
                            doc['observation']
                        )
                elif 'observations' in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts'],
                            doc['observations']
                        )
                elif 'observations' not in doc and 'observation' not in doc:
                    if 'forecasts' in doc:
                        converted[doc['_id']] = Instant(
                            doc['_id'],
                            doc['forecasts']
                        )
                else:
                    continue
            return converted
    except KeyError as e:
        print(f'KeyError in instant.convert(): {e}')
        
        return converted
        
def cast_count_all(instants):
    ''' get a tally for the forecast counts per document 

    :param instants: docmuments loaded from the db.instants collection
    Instant class objects
    :type instants: list
    '''
    
    n = 0
    collection_cast_counts = {}

    # Go through each doc in the collection and count the number of items in
    # the forecasts array. Add to the tally for that count.
    for doc in instants:
        if 'forecasts' in doc:
            n = len(doc['forecasts'])
        else:
            print('doc did not have forecasts as a key.')
            continue
        if n in collection_cast_counts:
            collection_cast_counts[n] += 1
        else:
            collection_cast_counts[n] = 1
    return collection_cast_counts


def sweep(instants):
    ''' Move any instant that has a ref_time less than the current next
    ref_time and with self.count less than 40. This is getting rid of the
    instants that are not and will not ever be legit.

    :param instants: a itterable of Instant objects
    :type instants: dict, list, pymongo cursor
    '''
    
    import time
    
    import pymongo
    from pymongo.cursor import Cursor

    import config
    import db_ops
    
    col = db_ops.dbncol(
        config.client,
        config.instants_collection,
        config.database
    )
    n = 0
    # Check the instant type- it could be a dict if it came from the database,
    # or it could be a list if it's a bunch of instant objects, or a pymongo
    # cursor on the collection.
    if type(instants) == dict:
        for key, doc in instants.items():
            if key['instant'] < time.time()-453000:  # 453000sec: about 5 days
                col.delete_one(doc)
                n += 1
    elif type(instants) == list:
        for doc in instants:
            if doc['instant'] < time.time()-453000:
                col.delete_one(doc)
                n += 1
    elif type(instants) == pymongo.cursor.Cursor:
        key = str
        for doc in instants:                
        
        ### This can probably be deleted if all the past collected data has
        ### been processed.
        # Check for the different likely keys in the documents and set the key
        # to the key that is there. for doc in instants:
            if 'instant' in doc:
                key = 'instant'
                instant = int(doc['instant'])
            elif 'observation' in doc:
                doc['observations'] = doc['observation']
                kee = 'observations'
                if 'timeplace' in doc:
                    instant = int(doc[kee]['weather']['timeplace'][:-10:-1])
                if '_id' in doc:
                    # print(doc['_id'])
                    instant = int(doc['_id'][:-10:-1])
            elif 'observations' in doc:
                kee = 'observations'
                if 'timeplace' in doc:
                    instant = int(doc[kee]['weather']['timeplace'][:-10:-1])
                if '_id' in doc:
                    val = doc[kee]['weather']['_id'][:-10:-1]
                    instant = int(val)                
            elif 'forecasts' in doc:
                kee = 'forecasts'
                for d in doc[kee]:
                    if 'timeplace' in d:
                        instant = int(d['timeplace'][:-10:-1])
                    if '_id' in d:
                        instant = int(d['_id'][:-10:-1])
            elif 'errors' in doc:
                return
            else:
                print('This document cannot be processed by sweep(). It does \
                not have these keys: instant, observation, observations, \
                forecasts')
                print(f'From {col}', '\n', doc)
                return
        ### This can probably be deleted if all the past collected data has
        ### been processed and just leave the following if statement.
        
            if instant < time.time()-500000:
                col.delete_one(doc)
                n += 1
    else:
        print(f'You want me to sweep instants that are {type(instants)}\'s.')
    return

def find_legit(instants, and_load=True):
    ''' find the 'legit' instants within the list

     :param instants: all the instants pulled from the database
     :type instants: dict of dicts
     :return: list of instants with a complete forecasts array
     '''
    
    inst = convert(instants) # Convert the values of instants to Instants
    del instants
    # Make a load list out of the Instants
    if and_load:
        legit_load_list = []
        for key, value in inst.items():
            if value.itslegit:
                legit_load_list.append(update_command_for(value.as_dict))
#         print(f'Got the legit_load_list and it is this long: {len(legit_load_list)}')
        if len(legit_load_list) != 0:
            load_legit(legit_load_list)
        del legit_load_list

    # Make a lisf of legit instants and return it.
    legit_dict = {}
    for key, value in inst.items():
        if value.itslegit:
            legit_dict[value.timeplace] = value.as_dict
    return legit_dict

def load_legit(legit):
    ''' Load the 'legit' instants to the remote database and delete from temp.
    This process does not delete the documents upon insertion, but rather holds
    it until the next time, tries to load it then, and finally, when getting a
    duplicate key error, deletes the document from its temporary location.

    :param legit: a single document or a list of update commands
    :type legit: dict or list
    '''

    import config
    import db_ops
    
    # make the remote and local clients from the config file for dbncol()
    remote_col = db_ops.dbncol(
        config.remote_client,
        config.legit_instants,
        config.database)
    col = db_ops.dbncol(
        config.client,
        config.instants_collection,
        config.database)

    if not isinstance(legit, dict):
        try:
            remote_col.bulk_write(legit)
        except pymongo.errors.InvalidOperation as e:
            print(e)
            return -1
        except TypeError as e:
            print(e)
            return -1
    else:
        try:
            remote_col.insert_one(legit)
            print(f'from {type(legit)}, loaded legit')
        except pymongo.errors.DuplicateKeyError:
            col.delete_one(legit)
    return


if __name__ == '__main__':
    ''' Connect to the database, then move all the legit instants to the remote
    database and clear out any instants that are past and not legit.
    '''
    
    import time

    import config
    import make_instants
    import db_ops

    start_time = time.time() # This is to get the total runtime if this script is
                             # run as __main__
    make_instants.make_instants(
        config.client,
        config.forecast_collection,
        config.observation_collection,
        config.instants_collection
    )
    print('Database sweep in progress...')
    collection = db_ops.dbncol(   # The local collection
        config.client,
        config.instants_collection,
        config.database
    )
    col = db_ops.dbncol(   # The remote collection
        config.remote_client,
        config.legit_instants,
        config.database
    )

    ### Add the different filters that might help get all the differnt docs
    Filters = [
        {'instant': {'$exists': True}},
        {'_id': {'$exists': True}},
        {'observations': {'$exists': True}},
        {'observation': {'$exists': True}},
        {'forecasts': {'$exists': True}},
        {'timeplace': {'$exists': True}}
    ]
    for filters in Filters:
        # print(f'Looping with {filters}')
        # col_count = collection.count_documents({})
        col_count = collection.count_documents(filters)
        if col_count == 0:
            continue
        n = 0
        while n <= col_count:
            if n%10000 == 0:
                print(f'working..... n={n}')
            # find_legit(collection.find({})[n:n+100], and_load=True)  
            find_legit(collection.find(filters)[n:n+100], and_load=True)  
            n += 100
        try:
            # find_legit(collection.find({})[:n], and_load=True)
            find_legit(collection.find(filters)[:n], and_load=True)
        except:
            print('there was some exception')
        sweep(collection.find(filters).batch_size(100))
    ### Add the different filters that might help get all the differnt docs
    
    inst_count = collection.count_documents({})
    i = 0
    while i+100 < inst_count:
#         print('processing casts')
        col = collection.find({})[i:i+100]
        sweep(col)
        i += 100
    print(f'Total sweep time was {time.time()-start_time} seconds')
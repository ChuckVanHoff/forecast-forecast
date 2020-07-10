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
        
        self.count()
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
    ### THIS FUNCTION IS UNPROVEN ###
    ''' Convert a list of instants to instant.Instant objects.
    
    :param instants: dict of instants
    
    :return: dict of instant.Instant objects
    '''
    converted = {}
    for key, value in instants:
        converted[key] = instant.Instant(
            value['timeplace'],
            value['forecasts'],
            value['observation']
        )
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
        n = len(doc['forecasts'])
        if n in collection_cast_counts:
            collection_cast_counts[n] += 1
        else:
            collection_cast_counts[n] = 1
    return collection_cast_counts


def sweep(instants):
    ### THIS FUNCTION IS DOING NOTHING IF IT REFERS TO ANY 'instant' KEY ###
    ''' Move any instant that has a ref_time less than the current next
    ref_time and with self.count less than 40. This is getting rid of the
    instants that are not and will not ever be legit.

    :param instants: a itterable of Instant objects
    :type instants: dict, list, pymongo cursor
    '''
    
    import time
    
    import pymongo
    from pymongo import MongoClient
    from pymongo.cursor import Cursor
    import config
    import db_ops
    
    client = MongoClient('localhost', 27017)
    col = db_ops.dbncol(client, 'instant_temp', config.database)
    n = 0
    # Check the instant type- it could be a dict if it came from the database,
    # or it could be a list if it's a bunch of instant objects, or a pymongo
    # cursor over the database.
    if type(instants) == dict:
        for key, doc in instants:
            if key['instant'] < time.time()-453000:  # 453000sec: about 5 days
                col.delete_one(doc)
                n += 1
    elif type(instants) == list:
        for doc in instants:
            if doc['instant'] < time.time()-453000:
                col.delete_one(doc)
                n += 1
    elif type(instants) == pymongo.cursor.Cursor:
        for doc in instants:
            if doc['instant'] < time.time()-453000:
                col.delete_one(doc)
                n += 1
    else:
        print(f'You want me to sweep instants that are {type(instants)}\'s.')
    return

def find_legit(instants, and_load=True):
    ''' find the 'legit' instants within the list

     :param instants: all the instants pulled from the database
     :type instants: list
     :return: list of instants with a complete forecasts array
     '''
    
    legit_dict = {}
    legit_load_list = []
    instants = convert(instants)
    # Make a load list out of the Instants
    if and_load:
        for key, value in instants:
            if value.itslegit:
                legit_load_list.append(update_command_for(value.as_dict))
        print(f'Got the legit_load_list and it is this long: {len(legit_load_list)}')
        if len(legit_load_list) == 0:
            pass
        load_legit(legit_load_list)
    # Make a lisf of legit instants and return it.
    for key, value in instants:
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

    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError, InvalidOperation

    import config
    import db_ops
    
    client = MongoClient('localhost', 27017)
    remote_col = db_ops.dbncol(db_ops.Client(config.uri),
                               'legit_inst',
                               config.database
                              )
    col = db_ops.dbncol(client,
                        'instant_temp',
                        config.database
                       )
    if not isinstance(legit, dict):
        try:
            remote_col.bulk_write(legit)
        except InvalidOperation as e:
            print(e)
            client.close()
            return -1
    else:
        try:
            remote_col.insert_one(legit)
            print(f'from {type(legit)}, loaded legit')
        except DuplicateKeyError:
            col.delete_one(legit)
    client.close()
    return


if __name__ == '__main__':
    ''' Connect to the database, then move all the legit instants to the remote
    database and clear out any instants that are past and not legit.
    '''
    
    import time
    
    import config
    import db_ops

    start_time = time.time() # This is to get the total runtime if this script is
                             # run as __main__
    print('Database sweep in progress...')
    collection = 'instant_temp'
    col = db_ops.dbncol(db_ops.Client(config.uri),
                        collection,
                        config.database)
    instants = db_ops.read_mongo_to_dict(col)
    find_legit(instants, and_load=True)
#     col.bulk_write(load_list)
    sweep(col.find({}))
    print(f'Total sweep time was {time.time()-start_time} seconds')
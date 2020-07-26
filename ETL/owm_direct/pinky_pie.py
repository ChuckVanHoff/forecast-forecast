import pymongo

import config
import db_ops

class Instant:
    ''' The Instant is an object built out of a collection of Weathers: in the
    case of forecast-forecast there are 40 forecast dictionaries contained in a
    list and one observation dictionary for comparison.
    '''

    def __init__(self, _id, data={}):
        
        self._id = _id
        self.data = data
    
    @property
    def count(self):
        ''' Get the count of the elements in self.casts. '''
        
        return len(self.data)
    
    @property
    def itslegit(self):
        ''' Check the instant's weathers array's count and if it is 40, then
        True is returned, otherwise it returns False.

        :return: Boolian value (True or False)
        '''
        
        if self.count == 41 and 'obs' in self.data:
            return True
        else:
            return False
    
    def to_dbncol(self, and_delete=False):
        ''' Load the data to the database. 

        :param and_delete: Should the instant be checked for completion
        viability and deleted if it is no longer viable??? ask and_delete.
        :type and_delete: bool
        '''
        
        if self.itslegit():
            col = db_ops(
                config.remote_client,
                config.legit_instants,
                config.database
            )
            col.insert(self.data)
        elif and_delete:
            col = db_ops(
                config.remote_client,
                config.instants_collection,
                config.database
            )
            if 'instant' in self.data:
                if self.data['instant'] < time.time()-453000:
                    col.delete_one(self.data)
        else:
            col = db_ops(
                config.remote_client,
                config.instants_collection,
                config.database
            )
            col.update_one({'_id': self._id},
                           {'$set': self.data},
                           upsert=True
                          )
        return


def convert(instants):
    ''' Convert a list of instants to instant.Instant objects.
    
    :param instants: the instants
    :type instants: This function takes a pymongo.cursor.Cursor #is able to handle multiple types-
        # dict: must be a dict of dicts and sets the instant timeplace to
        #     whatever the primary keys are.
        # pymongo.collection.Collection: the collection will be converted to a
        #     cursor. It must have the same data fields that the dictionary must.
        pymongo.cursor.CursorType: A cursor over a collection query result.
            It has to have the same data that the dict has to have.
            
    :return: dict of instant.Instant objects
    '''
    
    converted = {}
    try:
        if type(instants) == pymongo.cursor.Cursor:
            # print('instant.convert() working on a pymongo.cursor.Cursor')
            for doc in instants:
                if 'type' in doc:
                    converted[doc['_id']] = Instant(doc['_id'], doc)
                else:
                    continue
            return converted
        else:
            print(f'from owm_direct.pinky_pie.convert: I am looking for a \
            pymongo cursor, not a {type(instants)}')
            exit()
    except KeyError as e:
        print(f'KeyError in pinky_pie.convert(): {e}')
    return converted
        
def cast_count_all(instants):
    ''' get a tally for the forecast counts per document 

    :param instants: a cursor on the db.instants collection
    :type instants: pymongo.cursor.Cursor
    '''
    
    if not isinstance(instants, pymongo.cursor.Cursor):
        raise ValueError(f'instants is a {type(instants)}..gotta be a cursor')
        
    collection_cast_counts = {}

    # Go through each doc in the collection and count the number of items in
    # the forecasts array. Add to the tally for that count.
    for doc in instants:
        n = len(doc)
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

    if not isinstance(instants, pymongo.cursor.Cursor):
        raise ValueError(f'instants is a {type(instants)}..gotta be a cursor')
    
    for doc in instants:
        if doc['instant'] < time.time()-453000:
            col.delete_one(doc)
    return

def find_legit(instants, and_load=True):
    ''' find the 'legit' instants within the list

     :param instants: all the instants in the database
     :type instants: pymongo.cursor.Cursor
     :param and_load: Determines if the legit instants will be loaded or not.
     :type and_load: bool
     
     :return: When and_load is false, returns a dictionary of legit instants.
     Otherwise just loads, no returns.
     '''
    
    inst = convert(instants) # Convert the values of instants to Instants

    # Make a load list out of the legit Instants and load that list to the db.
    if and_load:
        legit_load_list = []
        for key, value in inst.items():
            if value.itslegit:
                legit_load_list.append(pymongo.InsertOne(data))
        if len(legit_load_list) != 0:
            result = load_legit(legit_load_list)
            if result == -1:
                print('There was a problem loading legits to remote.')
        return

    # Make a lisf of legit Instants and return it.
    legit_dict = {}
    for key, value in inst.items():
        if value.itslegit:
            legit_dict[value._id] = value.data
    return legit_dict

def load_legit(legit):
    ''' Load the 'legit' instants to the remote database and delete from temp.
    This process does not delete the documents upon insertion, but rather holds
    it until the next time, tries to load it then, and finally, when getting a
    duplicate key error, deletes the document from its temporary location.

    :param legit: a single document or a list of update commands
    :type legit: dict or list
    
    :return: -1 if there was a problem with the load, otherwise nothing.
    '''
    
    # Make the remote and local collections.
    remote_col = db_ops.dbncol(
        config.remote_client,
        config.legit_instants,
        config.database)
    col = db_ops.dbncol(
        config.client,
        config.instants_collection,
        config.database)

    # Load the data to the remote database and delete it from the local when
    # there is already a copy in the remote.
    if not isinstance(legit, dict):
        try:
            remote_col.bulk_write(legit)
            print(f'from pinky_pie.load_legit, bulk loaded legit')
        except pymongo.errors.InvalidOperation as e:
            print(e)
        except TypeError as e:
            print(e)
        except:
            return -1
    else:
        try:
            remote_col.insert_one(legit)
            print(f'from pinky_pie.load_legit, loaded single legit')
        except pymongo.errors.DuplicateKeyError:
            col.delete_one(legit)
    return


if __name__ == '__main__':
    ''' Connect to the database, then move all the legit instants to the remote
    database and clear out any instants that are past and not legit.
    '''
    
    import time
    
    from make_instants import update_command_for

    start_time = time.time() # This is to get the total runtime if this script is
                             # run as __main__
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
    col_count = collection.count_documents({})
    n = 0
    while n+100 <= col_count:
        if n%10000 == 0:
            print(f'working..... n={n}')
        find_legit(collection.find({})[n:n+100], and_load=True)  
        n += 100
    find_legit(collection.find({})[n:col_count], and_load=True)
    sweep(collection.find({}).batch_size(100))
    print(f'Total sweep time was {time.time()-start_time} seconds')
        
''' Carry out the ETL process for OpenWeatherMaps data. '''


import os

import pymongo

import config
import geo_hash
import pinky
import db_ops


count = 0
hash_list = [] # a list of geohash coordinates
locations =  [] # a list of lat-lon coordinatesgeo_hash.decode(hash_list)
path = str # this is the progress log file name
lim = int # the number of locations requested from the list of locatoins
max_tries = int  # Limit the number of tries to complete the process.
dump = str # the shell command to execute a database dump operation
restore = str # the shell command to execute a mongorestore operation
client = object # an instance of pymongo.MongoClient()
coll = object # the database and collection

# # Check for a progress log. If there is one, then compare it to the locations
# # list and throw out all the ones that are on the progress list, then finish
# # getting the data for the timeplaces left over in the timeplaces list.
# if os.path.exists(path):
#     with open(path, 'r') as f:
#         for loc in f:
#             if loc in locations:
#                 locations.remove(loc)
#                 print(f'just removed {loc} from the locations list')
#         lim = len(locations)
#     while count < lim:
#         try:
#             pinky.party(locations[:lim], client=config.remote_client)
#             break
#         except KeyboardInterrupt:
#             break
#         except pymongo.errors.OperationFailure as of:
#             print(f'''Got an error in main before __main__: {of}
#             The next command is a mongodump, hoping to open up some quota.''')
#             result = os.system(dump)
#             if result == 0:
#                 print('mongodump complete. Continuing with the party :)')
#                 coll.drop()
# #             db = config.remote_client[config.database][config.weathers_collection]
#         except:
#             print('Got error. Restarting from the end of the log file.')
#             # Incriment the counter before continuing the loop at the last element.
#             count = 0
#             for line in open(path, 'r'):
#                 count += 1
#     os.remove(path)  # Because that's all handled and I need a clean log for
#                     # the coming main code.


if __name__ == '__main__':
    # Set the variables
    hash_list = geo_hash.make()
    locations = geo_hash.decode(hash_list)
    path = 'progress_log.txt'
    lim = config.limit
    max_tries = 5
    dump = f'/usr/local/bin/mongodump --db={config.database}'
    restore = f'/usr/local/bin/mongorestore --db={config.database} /Volumes/forecast\ data/owm_10052024/'
    client = config.client
    coll = client[config.database][config.weathers_collection]
    
    # Start a loop that will continue until all locations have been requested
    # for, making sure that when an error occurs that is not handled otherwise
    # the pinky party restarts where is last had a success.
    while count < max_tries: 
        if db_ops.check_db_access(client):
            # If there is a progress log, open it and remove all the locations
            # from locations that are in the progress log.
            if os.path.exists(path):  # Checking this in case there was an
                                        # error that sent me back through.
                print('there is a progress log in the dir...updating.')
                with open(path, 'r') as f:
                    for loc in f:
                        if loc in locations:
                            locations.remove(loc)
                            print(f'just removed {loc} from the locations list')
            else:
                # Create an empty log file:
                with open(path, 'w') as pl:
                    print('Started new progress log.')
            # Do the process now with all the values remaining in locaitons.
            try:
                pinky.party(locations[:lim], client=client, load_raw=True)
                os.remove(path)  # Delete the progress log.
                break
            except KeyboardInterrupt:
                break
            except pymongo.errors.OperationFailure:
                result = os.system(dump)
                if result == 0:
                    print('mongodump complete. Dropping the collection and continuing with the party :)')
                    coll.drop()
                    # os.remove(path)  # Delete the progress log.
                    count += 1
                    continue
            except:
                print('Got an unanticipated error. Restarting pinky from end of log file.')
                count += 1
                continue
            count += 1
        else:
            print("you have no db access!")
    # Check the database to be sure the data was loaded, then try to dump it.
    # If the dump was a success, run a restore command so that the data will
    # not be overwritten during the next dump. then try to drop it.
    # doc_count = coll.count_documents({})
    # if doc_count > 0:
    #     print(doc_count)
    #     try:
    #         os.system(dump) # This should typically dump from the remote client
    #                         # to local storage.
            # result = client[config.database][config.weathers_collection].count_documents({})
#             if result == 0:
#                 print('dumped the collection. Now restoring it to the local.')
#                 os.system(restore) # This should typically restore from a local
#                                     # storage to a local mongo database.
#                 ### GET A CHECK FOR COMMAND SUCCESS ###
#                 if result == 0:
#                     print('I think the collection was restored. Now dropping col.')
#                 else:
#                     print(f'The restore failed... system result was {result}.')
#                 ### GET A CHECK FOR COMMAND SUCCESS ###

#                 coll.drop()
#                 try:
#                     num = coll.count_documents({})
#                     if num == 0:
#                         print('dropped the collection')
#                     else:
#                         print(f'There are still {num} docs in the collection.')
#                 except:
#                     print('Check the oplog to see that coll was restored.')
#             else:
#                 print(f'''it did not return anything from the command. I was
#                         expecting a true/false return.
#                         result = {result}''')
        # except:
        #     print('just got an error in main.py while trying to dump and drop')
    else:
        print(f'at end of main.py, and there were no documents in the checked collection {coll}')

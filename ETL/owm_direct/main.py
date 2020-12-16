''' Carry out the ETL process for OpenWeatherMaps data. '''


import os

import pymongo

import config
import geo_hash
import pinky
import db_ops


hash_list = geo_hash.make()
locations = geo_hash.decode(hash_list)
lim = len(locations)
# lim = 165
count = 0
path = 'progress_log.txt'
dump = f'/usr/local/bin/mongodump --uri={config.uri}'
restore = f'/usr/local/bin/mongorestore dump'
coll = config.remote_client[config.database][config.weathers_collection]

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
    
    # Start a loop that will continue until all the locations have been requested
    # for, making sure that when an error occurs that is not handled otherwise
    # the pinky party restarts where is last had a success.
    count = 0
    print(f'count = {count}, lim = {lim}')
    while count < lim:
        if db_ops.check_db_access(config.remote_client):
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
                pinky.party(locations[:lim], client=config.remote_client)
                break
            except KeyboardInterrupt:
                break
            except pymongo.errors.OperationFailure:
                result = os.system(dump)
                if result == 0:
                    print('mongodump complete. Dropping the collection and continuing with the party :)')
                    coll.drop()
            except:
                print('Got error. Restarting pinky.    from the end of the log file.')
                # Incriment the counter before continuing the loop at the last element.
                for line in open(path, 'r'):
                    count += 1
        else:
            print("you have no db access!")
    # Check the database to be sure the data was loaded, then try to dump it.
    # If the dump was a success, run a restore command so that the data will
    # not be overwritten during the next dump. then try to drop it.
    if coll.count_documents({}) > 0:
        try:
            print('just about to try to give the mongodump command from main.py')
            result = os.system(dump)
            if result == 0:
                print('dumped the collection. Now restoring it to the local.')
                os.system(restore)
                # GET A CHECK FOR COMMAND SUCCESS
                print('I think the collection was restored. Now dropping col.')
                coll.drop()
                try:
                    num = coll.count_documents({})
                    if num == 0:
                        print('dropped the collection')
                    else:
                        print(f'There are still {num} docs in the collection.')
                except:
                    print('Check the oplog to see that coll was restored.')
                os.remove(path)  # Drop the progress log file.
            else:
                print(f'''it did not return anything from the command. I was 
                        expecting a true/false return.
                        result = {result}''')
        except:
            print('just got an error in main.py while trying to dump and drop')

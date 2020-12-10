''' Carry out the ETL process for OpenWeatherMaps data. '''

import os

import config
import geo_hash
import pinky
import db_ops

hash_list = geo_hash.make()
locations = geo_hash.decode(hash_list)
lim = len(locations)
# lim = 165
count = 0
timeplaces = []

path = 'progress_log.txt'
# Check for a progress log. If there is one, then compare it to the locations
# list and through out all the ones that are on the progress list, then finish
# getting the data for the timeplaces left over in the timeplaces list.
if os.path.exists(path):
    with open(path, 'r') as f:
        for loc in f:
            if loc in locations:
                locations.remove(loc)
                print(f'just removed {loc} from the locations list')
        lim = len(timeplaces) - 1
    while count < lim:
        try:
            pinky.party(locations[count:lim], client=config.remote_client)
            break
        except KeyboardInterrupt:
            break
        except pymongo.errors.OperationFailure:
            db = config.remote_client[config.database][config.weathers_collection]
        except:
            print('Got error. Restarting from the end of the log file.')
            # Incriment the counter before continuing the loop at the last element.
            count = 0
            for line in open('progress_log.txt', 'r'):
                count += 1
    count = 0
    os.remove(path)
# Check the remote database. If there are documents in there, dump them and
# drop the collection
coll = config.remote_client[config.database][config.weathers_collection]
command = f'mongodump --uri={config.uri}'
#mongodb+srv://chuckvanhoff:Fe7ePrX%215L5Wh6W@cluster0-anhr9.mongodb.net/test'


if __name__ == '__main__':
    # Create an empty log file:
    with open(path, 'w') as pl:
        print('Started new progress log.')
    
    # Start a loop that will continue until all the locations have been requested
    # for, making sure that when an error occurs that is not handled otherwise
    # the pinky party restarts where is last had a success.
    while count < lim:
        if db_ops.check_db_access(config.remote_client):
            try:
                pinky.party(locations[count:lim], client=config.remote_client)
                break
            except KeyboardInterrupt:
                break
            except pymongo.errors.OperationFailure:
                db = config.remote_client[config.database][config.weathers_collection]
            except:
                print('Got error. Restarting pinky.    from the end of the log file.')
                pinky.party(locations[count:lim], client=config.remote_client)
                # Incriment the counter before continuing the loop at the last element.
                count = 0
                for line in open(path, 'r'):
                    count += 1
        else:
            print("you have no db access!")
    # Check the database to be sure the data was loaded, then try to dump it and
    # if the dump was a success, then try to drop it.
    if coll.count_documents({}) > 0:
        try:
            print('just about to try to give the mongodump command from main.py')
            result = os.system(command)
            if result == 0:
                print('dumped the collection.')
                coll.drop()
                print('dropped the collection')
            else:
                print(f'''it did not return anything from the command. I was 
                        expecting a true/false return.
                        result = {result}''')
        except:
            print('just got an error in main.py while trying to dump and drop')
    os.remove(path)  # Drop the progress log file since the process completed.

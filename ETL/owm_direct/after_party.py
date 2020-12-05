''' Move the data from the remote database to the local database and 
erase the moved documents from the database. Also update the timeplace
collection log.
'''


import os
import subprocess
import json
from collections import defaultdict

import config
import db_ops


timeplace_record = {}
command = f'mongodump --uri={config.uri}'
#mongodb+srv://chuckvanhoff:Fe7ePrX%215L5Wh6W@cluster0-anhr9.mongodb.net/'
remote_col = config.remote_client[config.database][config.weathers_collection]
local_col = config.client[config.database][config.weathers_collection]


if __name__ == '__main__':
    print('Starting the after party.')
    # Load the timeplace record and store it in a defaultdict to be updated.
    if os.path.exists('timeplace_record.json'):
        print('timeplace_record.json was found')
        with open('timeplace_record.json', 'r') as fp:
#             temp = json.load(fp)
            timeplace_record = defaultdict(int, json.load(fp))#temp)
    else:
        print('timeplace_record.json was NOT found')
        timeplace_record = defaultdict(int)
#     try:
#         temp = {}
#         with open('timeplace_record.json', 'r') as fp:
#             temp = json.load(fp)
#         timeplace_record = defaultdict(int, temp)
#     except:
#         with open('timeplace_record.json', 'a') as fp:
#             print('Had to create timeplace_record.json.')
#         timeplace_record = defaultdict(int)
    # Update the timeplace collection record.
    docs = remote_col.find({})
    for doc in docs:
        timeplace = str(doc['timeplace'])
        # Incriment the value of the key before trying to add it.
        timeplace_record[timeplace] += 1
#         try:
#             timeplace_record[timeplace] += 1
#         except KeyError as ke:
#             print(ke)
#             print('got that keyerror again...{timeplace}')
#             timeplace_record[timeplace] = 0
    with open('timeplace_record.json', 'w') as rec:
        json.dump(timeplace_record, rec)
    # Dump the collection and drop it from the remote.
    try:
        count = remote_col.count_documents({})
        if count != 0:
            print(f'There are {count} documents in {remote_col}')
#            print('Currently not dumping ro dropping the collection')
#            if os.system(command):
            if subprocess.call(command, shell=True) == 0:
                print('''The os.system() function returned a True, so I am assuming we
                        Successfully performed the mongodump command.''')
            remote_col.drop()
            print('Dropped the remote database, I think, but i"m not sure.')
        else:
            print(f'Your collection, {remote_col}, was empty at first check.')
    except:
        # This is in case the process did not finish clearing the remote database..
        print('''There was an exception of some sort in after_party.py.
              Trying to go about it with copy_docs().''')
        db_ops.copy_docs(
            remote_col,
            config.client,
            config.database,
	    config.weathers_collection,
            filters={},

            delete=True
        )


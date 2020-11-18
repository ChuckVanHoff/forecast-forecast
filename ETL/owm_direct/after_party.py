''' Move the data from the remote database to the local database and 
erase the moved documents from the database. Also update the timeplace
collection log.
'''


import os
import json
from collections import defaultdict

import config
import db_ops


timeplace_record = {}
command = 'mongodump --uri=mongodb+srv://chuckvanhoff:Fe7ePrX%215L5Wh6W@cluster0-anhr9.mongodb.net/test'
remote_col = config.remote_client[config.database][config.weathers_collection]
local_col = config.client[config.database][config.weathers_collection]


if __name__ == '__main__':
    # Load the timeplace record and store it in a defaultdict to be updated.
    try:
        with open('timeplace_records.json', 'r') as fp:
            tprec = json.load(fp)
        timeplace_record = defaultdict(int, tprec)
        # Update the timeplace collection record.
    except:
        with open('timeplaces_records.json', 'a') as fp:
            print('Had to create timeplaces_records.json.')
            tprec = {}
    docs = remote_col.find({})
    for doc in docs:
        timeplace = str(doc['timeplace'])
        timeplace_record[timeplace] += 1
    with open('timeplace_records.json', 'w') as rec:
        json.dump(timeplace_record, rec)

    # This is in case the process did not finish clearing the remote database..
    try:
        if remote_col.count_documents({}) != 0:
            os.system(command)
            print('Successfully performed the mongodump command.')
            remote_col.drop()
            print('Successfully dropped the remote database.')
        else:
            print(f'Your collection, {remote_col}, was empty at first check.')
    except:
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

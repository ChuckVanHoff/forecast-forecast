''' Move the data from the remote database to the local database and 
erase the moved documents from the database. Also update the timeplace
collection log.
'''


import os
import json
from collections import defaultdict

import config
import db_ops


command = 'mongodump --uri=mongodb+srv://chuckvanhoff:Fe7ePrX%215L5Wh6W@cluster0-anhr9.mongodb.net/test'
remote_col = config.remote_client[config.database][config.weathers_collection]
local_col = config.client[config.database][config.weathers_collection]
# This is in case the process did not finish clearing the remote database..
try:
    if remote_col.count_documents({}) != 0:
        os.system(command):
        print('performed the command.')
    else:
        print(f'{remote_col} is empty. Proceding with the weather collection.')
except:
    print('there was an exception')
    pass
# Load the timeplace record and store it in a defaultdict to be updated.
try:
    with open('timeplace_records.json', 'r') as fp:
        tprec = json.load(fp)
except:
    with open('timeplaces_records.json', 'a') as fp:
        print('Had to create timeplaces_records.json.')
        tprec = {}
timeplace_record = defaultdict(int, tprec)


if __name__ == '__main__':
    # Update the timeplace collection record.
    docs = remote_col.find({})
    for doc in docs:
        timeplace = str(doc['timeplace'])
        timeplace_record[timeplace] += 1

    db_ops.copy_docs(
        remote_col,
        config.client,
        config.database,
        config.weathers_collection,
        filters={},
        delete=True
    )
    with open('timeplace_records.json', 'w') as rec:
        json.dump(timeplace_record, rec)

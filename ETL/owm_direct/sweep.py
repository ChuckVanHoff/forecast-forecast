''' Remove all the timeplaces that cannot be legitemized and pass over all
the rest. Make lists of all the timeplaces of each type.
'''

import time

import config
import db_ops


# Setup the database connection and name the collection.
client = config.client
db = client[config.database]
col = db[config.weathers_collection]

cursor = col.find({})
timeplaces = set([doc['timeplace'] for doc in cursor])
print(f'there are {len(timeplaces)} timeplaces.')
# 
for timeplace in timeplaces:
    if col.count_documents({'timeplace': timeplace}) > 14:
        continue
    now = int(time.time())
    _time = int(timeplace[-10:])
    if _time < now - 10800*41:
        db_ops.copy_docs(
            col,config.database,
            config.weathers_archive,
            filters={'timeplace': timeplace},
            delete=True
        )

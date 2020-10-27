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

# Initialize the values of variables used in the while loop.
m = 0  # The lower limit of the slice from timeplaces
n = 0  # The upper limit of the slice from timeplaces
dels = 0  # The number of documents documented from the database
l = len(timeplaces) 

cursor = col.find({})
test_total = col.count_documents({})
print(f'Total number of documents to process: {test_total}')
timeplaces = list(set([doc['timeplace'] for doc in cursor]))
print(f'Total nuber of timeplaces going into the main loop: {len(timeplaces)}')
# keep track of the number of timeplaces processed
while n < l:
#     if n % 10000 == 0:
#         print(f'processed {n} timeplaces so far. {100*n/l}%')
    m = n
    if n+100 <= l:
        n+=100
    else:
        # Set the value for the slice endpoint to the last element fo the list.
        n = l - 1
        l = 0
    for timeplace in timeplaces[m:n]:
        if col.count_documents({'timeplace': timeplace}) > 40:
            continue
        now = int(time.time())
        _time = int(timeplace[-10:])
        if _time < now - 10800*41:
            dels += 1
            db_ops.copy_docs(
                col,
                config.database,
                config.weathers_archive,
                filters={'timeplace': timeplace},
                delete=True
            )
print(f'processed {n} timeplaces and deleted {dels} documents.')

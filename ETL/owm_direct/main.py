''' Carry out the ETL process for OpenWeatherMaps data. '''

import config
import geo_hash
import pinky

hash_list = geo_hash.make()
locations = geo_hash.decode(hash_list)
# lim = len(locations) - 1
lim = 165
count = 0

# Create an empty log file:
with open('progress_log.txt', 'w') as pl:
    print('Started new progress log.')
# Start a loop that will continue until all the locations have been requested
# for, making sure that when an error occurs that is not handled otherwise
# the pinky party restarts where is last had a success.
while count < lim:
    try:
        pinky.party(locations[count:lim], client=config.remote_client)
        break
    except:
        print('Got error. Restarting from the end of the log file.')
        # Incriment the counter before continuing the loop at the last element.
        count = 0
        for line in open('progress_log.txt', 'r'):
            count += 1

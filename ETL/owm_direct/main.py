''' Carry out the ETL process for OpenWeatherMaps data. '''

import config
import geo_hash
import pinky
import pinky_pie

hash_list = geo_hash.make()
locations = geo_hash.decode(hash_list)
pinky.party(locations[:])
# pinky_pie.sweep(config.client[config.database][config.weathers_collection].find({}))

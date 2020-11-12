import geohash

def make(to_file=False):
    ''' Make a geohash list and return it. 
    You will have to adjust the first few digits to get the "center" point
    located, and the loop creates a list to the accuracy you want according
    to how many subloops you put in it.
    Adjust the values according to the geohash directions here:
    https://www.movable-type.co.uk/scripts/geohash.html
    
    :param to_file: Controls whether to write the hash list to a file or not.
    :type to_file: bool
    '''

    b32 = '0123456789bcdefghjkmnpqrstuvwxyz' # the values from a base 32 system
    hl = [f'dn{p3}{p4}{p5}' for p3 in b32[16:24] for p4 in b32 for p5 in b32]
    hl.sort()
    if to_file:
        with open('geohash_list.txt', 'w') as gh:
            for row in hl:
                gh.write(str(row) + '\n')
    return hl

def decode(hl):
    ''' Take a single of a list of geohashes and return a single list of
    decoded coordinate dicts. '''
    
    if isinstance(hl, str):
        return geohash.decode(hl)
    if isinstance(hl, list):
        locations = []  # Coordinate list
        for row in hl:
            cd = {}  # Coordinate dict
            cd['lat'] = geohash.decode(row)[0]
            cd['lon'] = geohash.decode(row)[1]
            locations.append(cd)
        return locations

def encode(cl):
    ''' Take a pair of or list of pairs of coordinate dicts and return a single
    or a list of geohashes. '''
    
    if isinstance(cl, dict) and 'lat' in cl and 'lon' in cl:
        return geohash.encode(cl['lat'], cl['lon'])
    if isinstance(cl, list):
        locations = []  # geohash list
        for row in cl:
            locations.append(geohash.encode(row['lat'], row['lon']))
        return locations
    
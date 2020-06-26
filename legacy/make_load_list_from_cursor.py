def make_load_list_from_cursor(cursor):
    ''' create the list of objects from the database to be loaded through
    bulk_write()
    
    :param cursor: it is just what the name says it is
    :type cursor: a pymongo cursor
    :return update_list: list of update commands for the weather objects on the
    cursor
    '''

    update_list = []
    
    # check the first entry to know whether it's forecast or observation
    if 'Weather' in cursor[0]:
        for obj in cursor:
            update_list.append(update_command_for(obj))
        return update_list
    else:
        print('Did not find weather or Weather in pymongoCursor')
        for obj in cursor:
            # Trying things that will capture any of the formats published over
            # the developemnet period.
            try:
                if 'reception_time':
                    update_list.append(update_command_for(cast))
                    print('found reception_time')
                    casts = obj['weathers'] # use the 'weathers' array from the
                                            # forecast
                    for cast in casts:
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant']\
                                                - obj['reception_time']
                        update_list.append(update_command_for(cast))
                    return update_list
                else:
                    casts = obj['weathers'] # use the 'weathers' array from the
                                            # forecast
                    for cast in casts:
                        # this is just setting the fields as I need them for
                        # each update object as it gets loaded to the database
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant']\
                                                - cast['reception_time']
                        update_list.append(update_command_for(cast))
                    return update_list
            except KeyError as e:
                print(f'from make_instants.make_load_list_from_cursor(): KeyError.args = {e.args}')
                print(e.with_traceback)
                return
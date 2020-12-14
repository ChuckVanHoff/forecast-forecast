def add_dir_to_path():
    ''' Add the __main__ module's directory to the system PATH. '''
    
    import os
    import sys
    import inspect
    
    currentdir = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe()
            )
        )
    )
    sys.path.insert(0, currentdir) 

def add_parent_to_path():
    ''' Add the __main__ module's parent directory to the system PATH. '''
    
    import os
    import sys
    import inspect
    
    currentdir = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe()
            )
        )
    )
    parentdir = os.path.dirname(currentdir)
    sys.path.insert(0, parentdir) 

# def add_chil_to_path()
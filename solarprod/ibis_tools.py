import contextlib
import os
import ibis
from . import postgres_tools as pgtools

from .constants import (
    PRODUCTION_CONN_NAME,
    ANALYITICS_CONN_NAME,
    LOCAL_CONN_NAME,
    LOCAL_DB_FILENAME,
) 


def get_local_connection(reset=False):
    """
    A function to get a connection to the local database
    Args: 
        reset: if set to True, will blow away any existing database file
    """
    file_name = LOCAL_DB_FILENAME
    if os.path.isfile(file_name) and reset:
        os.unlink(file_name)
    conn = ibis.duckdb.connect(file_name)
    return conn


@contextlib.contextmanager
def get_connections(*names):
    """
    A manager that will return connections to datasbases.
    It defines a dictionary of names along with callables to
    retrieve the associated connections.

    One of the main purposes of this manager is to make sure that sqlalchemy properly
    closes ibis connections when we drop out of the context.

    Args:
       *names: connections corresponding to supplied names will be returned in the
               same order they were specified
    """
    # The allowed connection names are taken from global variables
    allowed_connections = [
        PRODUCTION_CONN_NAME, 
        LOCAL_CONN_NAME, 
        ANALYITICS_CONN_NAME
    ]
    
    # Make sure all requested connections are valid
    bad_conns = set(names) - set(allowed_connections)
    if bad_conns:
        raise ValueError(f'valid connection names are {allowed_connections}')
        
    # Define the connection getters for each name
    getter_dict = {
        PRODUCTION_CONN_NAME: lambda: pgtools.get_postgres_ibis_connection('production'),
        ANALYITICS_CONN_NAME: lambda: pgtools.get_postgres_ibis_connection('analytics'),
        LOCAL_CONN_NAME: lambda: get_local_connection()
    }
    
    # Create all requested connections
    connections = [getter_dict[name]() for name in names]
    
    # Yield in a try block to ensure all connections are disposed after use
    try:
        if len(connections) == 1:
            yield connections[0]
        else:
            yield connections
    finally:
        # Make sure the sqlalchemy connections are closed
        for connection in connections:
            connection.con.dispose()

import datetime

import numpy as np
import fleming
import easier as ezr
from dateutil.relativedelta import relativedelta
import pandas as pd
import ibis
from ibis import _

from .constants import (
    LOCAL_CONN_NAME,
    PRODUCTION_CONN_NAME,
    EARLIEST_DATE,
    MIN_NEIGHBOR_MILES,
    MAX_NEIGHBOR_MILES,
    ANALYITICS_CONN_NAME,
)

from .ibis_tools import (
    get_connections,
)


def get_yesterday():
    """
    Several places in the code need access to "yesterday", so write a small
    utility to get that.
    """
    today = fleming.floor(datetime.datetime.now(), day=1)
    yesterday = today - relativedelta(days=1)
    return yesterday
    

def get_start_date(connection_or_connection_name, table_name, default_start_date=EARLIEST_DATE):
    """
    This function solves for the pattern of updating a database with records later than the latest
    record it already contains.  Its purpose is to retrieve the earliest date with no corresponding
    records in the table.

    Args:
        connection_or_connection_name: Either the name of a connection or the connection itself 
                                       that contains the table to be updated
                           table_name: The name of the table that will be updated
                   default_start_date: If no start date is found, return this date instead
    """
    # I never want to populate "today" because not everything for today has happened already
    yesterday = get_yesterday()
    
    # create a utility function that knows how to get start date from a connection
    def extract_starting_from_table(conn):
        # If the table exists
        if table_name in conn.list_tables():
            # Get an ibis table object
            table = conn.table(table_name)

            # Get the latest date that was pushed
            last_pushed_date = table.date.max().execute()
        
            # Set the start date to one day after the last pushed date
            start_date = last_pushed_date + relativedelta(days=1)
    
            # If asking for a date after yesterday, no valid start date
            if start_date > yesterday:
                start_date = None
        
        # If the table doesn't exist, then must populate all
        else:
            start_date = default_start_date
            
        return start_date
        
    # If a connection name was provided, get start date using that name        
    if isinstance(connection_or_connection_name, str):
        # Get the connection the table lives in
        with get_connections(connection_or_connection_name) as conn:
            start_date = extract_starting_from_table(conn)
    
    # Otherwise a connection was provided.  Get the start date using the connection            
    else:
        start_date = extract_starting_from_table(connection_or_connection_name)
    
    # Return the start date
    return start_date


def sync_homeowners():
    """
    This function wipes out the homeowners table in the local db and repoppulates
    it with values from production
    """
    with get_connections(PRODUCTION_CONN_NAME, LOCAL_CONN_NAME) as (production_conn, local_conn):
        # Get all homeowners that have coordinates specified
        homeowner_tablename = 'homeowners'
        homeowners = production_conn.table('homeowners')
        homeowners = homeowners['id', 'lat', 'lng'].relabel({'id': 'homeowner_id'})
        homeowners = homeowners[homeowners.lat.notnull() & homeowners.lng.notnull()]

        # Make sure the datatypes are right
        homeowners = homeowners.mutate(homeowner_id=homeowners.homeowner_id.cast('int'))
        homeowners = homeowners.mutate(lat=homeowners.lat.cast('float'), lng=homeowners.lng.cast('float'))

        # Grab a frame of the results and push it to local db
        df = homeowners.execute()
        local_conn.raw_sql(f"drop table if exists {homeowner_tablename}")
        local_conn.insert(homeowner_tablename, df)


def sync_prod_history(show_progress_bar=False, memory_friendly=True):
    """
    The history report table has daily production history for all homes at all times.
    This needs to be synced over to the local db, but it's a lot of data.

    Args:
        show_progress_bar: Set to True if you are running in a notebook and want to see a progress bar
        memory_friendly: If set to True, will make one call to the production db for each day.
                         Otherwise, it will ram the entire history table into memeory at once.
    """
    
    # Grab the databse connections
    with get_connections(PRODUCTION_CONN_NAME, LOCAL_CONN_NAME) as (production_conn, local_conn):

        # Don't want to do anything for today, since there is more that can still happen today
        yesterday = get_yesterday()
    
        # I will ignore all production levels below this number
        prod_threshold = 10

        # This is the name of the target table I am populating
        table_to_populate = 'prod_history'

        # Get the production history data and filter / clean it the way I like
        hist = production_conn.table('history_report')
        hist = hist['date', 'homeownerId', 'totalProduction']
        hist = hist.mutate(date=hist.date.cast('timestamp'))
        hist = hist.relabel(ezr.slugify(hist.columns, kill_camel=True, as_dict=True))
        hist = hist[hist.total_production > prod_threshold]
        hist = hist.sort_by(['date', 'homeowner_id'])

        # Get a start date for syncing from the target db 
        start_date = get_start_date(local_conn, table_to_populate)

        # If couldn't get valid start date, do nothing
        if start_date is None:
            return

        # Create a range of days over which to compute production
        days = pd.date_range(start_date, yesterday)

        # If you want to show progress bar, wrap in tqdm
        if show_progress_bar and memory_friendly:
            import tqdm.notebook as tqdm
            days = tqdm.tqdm(days)

        # Use this branch if you don't have enough memory to hold all production
        # for all homes within the specified date ranges.
        if memory_friendly:
            # Loop over all days, transfering data from production to target
            for day in days:
                batch = hist[hist.date == day]
                df_batch = batch.execute()
                if not df_batch.empty:
                    local_conn.insert(table_to_populate, df_batch)
        else:
            df_batch = hist[hist.date.between(start_date, yesterday)].execute()
            local_conn.insert(table_to_populate, df_batch)


def update_neighbors():
    """
    A key aspect of the detector is that it will mute itself if a bunch
    of neighboring homes also generate detections.  In order to do this, we need
    a list of neighbors and their distances.  This function populates the neighbors
    table.
    """
    # Grab the connection
    with get_connections(LOCAL_CONN_NAME) as local_conn:
        # We will be creating this table in the target database
        neighbor_table_name = 'neighbors'

        # Define the earth radius for computing distance
        r_earth_miles = 3963

        # Don't consider more than this many neighbors
        max_neighbors = 100

        # Distance range for eligible neighbors
        min_miles, max_miles = .125, 50
        min_miles, max_miles = MIN_NEIGHBOR_MILES, MAX_NEIGHBOR_MILES

        # Get a reference to the table and transform coords to radians
        homeowners = local_conn.table('homeowners')
        homeowners = homeowners.mutate(
            lat=_.lat * np.pi / 180,
            lng= _.lng * np.pi / 180
        )

        # Make two copies of homeowners with different names
        # (for some reason cross joining a table with itself didn't work in ibis)
        owners1 = homeowners.relabel({c: f'{c}1' for c in homeowners.columns})
        owners2 = homeowners.relabel({c: f'{c}2' for c in homeowners.columns})

        # Cross join the homeowner copies
        neighbors = owners1.cross_join(owners2)

        # A home can't be a neighbor to itself, so only keep record with different ids
        neighbors = neighbors.mutate(is_different=_.homeowner_id1 != _.homeowner_id2)
        neighbors = neighbors[neighbors.is_different].drop('is_different')

        # Compute approximate dx, dy distances in miles.  We are only using this
        # to limit which homes we look at, so accurate distances are really not important
        neighbors = neighbors.mutate(
            dy=r_earth_miles * (_.lat2 - _.lat1), 
            dx=r_earth_miles * _.lat1.cos() * (_.lng2 - _.lng1)
        )

        # Compute the distance between neighbors
        neighbors = neighbors.mutate(distance_miles=(_.dx ** 2 + _.dy ** 2).sqrt())
        neighbors = neighbors[
            'homeowner_id1',
            'homeowner_id2',
            'distance_miles'
        ]

        # Only keep homes between a min and max distance. Some homes are listed
        # multiple times under different ids, so you want to rule these out
        neighbors = neighbors[neighbors.distance_miles.between(min_miles, max_miles)]

        # Create a window that will group records by homeowner_id and order records in the group
        # by distance
        window = ibis.window(group_by=neighbors.homeowner_id1, order_by=neighbors.distance_miles)

        # Create an integer index of neighbors for each home that increases as a function of distance
        neighbors = neighbors.mutate(ind=ibis.row_number().over(window))

        # Only consider the N nearest neighbors, where N = max_neighbors
        neighbors = neighbors[neighbors.ind < max_neighbors].drop('ind')

        # Ibis does something funny with metadata that I don't understand.
        # Resetting the connection fixes it
        local_conn.reconnect()

        # Drop the neighbors table if it exists
        local_conn.raw_sql(f"drop table if exists {neighbor_table_name}")

        # Create the new neighbors table.  Note that data should never be sent over
        # the wire for this.  It's just a query that is used to create the table
        # all within the db.
        local_conn.create_table(neighbor_table_name, neighbors, force=True)


def get_unique_homes(start_date):
    """
    This function returns a list of unique homeowner ids that
    had production since the specified start_date
    """
    with get_connections(LOCAL_CONN_NAME) as local_conn:
        hist = local_conn.table('prod_history')
        hist = hist[hist.date > start_date]
        hist = hist[['homeowner_id']].distinct()
        hist = hist.sort_by('homeowner_id')
        return list(hist.homeowner_id.execute())


def push_detections():
    with get_connections(LOCAL_CONN_NAME, ANALYITICS_CONN_NAME) as (conn_local, conn_analytics):
        start_date = get_start_date(ANALYITICS_CONN_NAME, 'detections')
        if start_date is None:
            return
            
        detections = conn_local.table('detections')
        detections = detections[detections.date >= start_date]
        df = detections.execute()
        logger = ezr.get_logger('push_detections')
        logger.info(f'pushing {len(df)} detections')
        if not df.empty:
            conn_analytics.insert('detections', df)        

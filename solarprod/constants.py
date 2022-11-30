import pandas as pd

# Database connection stuff
PRODUCTION_CONN_NAME = 'production'
ANALYITICS_CONN_NAME = 'analytics'
LOCAL_CONN_NAME = 'local'
LOCAL_DB_FILENAME = '/tmp/solar.ddb'


# Data plumbing stuff
EARLIEST_DATE = pd.Timestamp('1/1/2020')
MIN_NEIGHBOR_MILES, MAX_NEIGHBOR_MILES = .125, 50


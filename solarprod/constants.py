import pandas as pd

# Database connection stuff
PRODUCTION_CONN_NAME = 'production'
ANALYITICS_CONN_NAME = 'analytics'
LOCAL_CONN_NAME = 'local'
LOCAL_DB_FILENAME = '/tmp/solar.ddb'


# Data plumbing stuff
EARLIEST_DATE = pd.Timestamp('1/1/2020')
MIN_NEIGHBOR_MILES, MAX_NEIGHBOR_MILES = .125, 50


# Detector stuff
SMOOTHING_DAYS = 14
LAG_DAYS = 14
SLOPE_THRESHOLD_RATIO = .6
NEIGHBOR_RADIUS_MILES = 50
NEIGHBOR_COUNT_THRESH = 4
NOMINAL_PROD_TABLE_NAME = 'nominal_prod'
RAW_DETECTION_TABLE_NAME = 'raw_detections'
DETECTION_TABLE_NAME = 'detections'

import pandas as pd
import numpy as np
import easier as ezr
from scipy import stats
from dateutil.relativedelta import relativedelta

from .data_plumbing import (
    get_connections,
    get_start_date,
    get_unique_homes,
)


from .constants import (
    EARLIEST_DATE,
    LOCAL_CONN_NAME,
    SMOOTHING_DAYS,
    LAG_DAYS,
    SLOPE_THRESHOLD_RATIO,
    NEIGHBOR_RADIUS_MILES,
    NEIGHBOR_COUNT_THRESH,
    NOMINAL_PROD_TABLE_NAME,
    RAW_DETECTION_TABLE_NAME,
    DETECTION_TABLE_NAME
)


class NominalProd:
    # I run a weighted smoothing of the production.  The raw
    # weights are just the percent rank.  These are then transformed
    # with a beta distribution to put more weight on the higher
    # productions.
    # alpha = smoother_n * smoother_ratio
    # beta = smoother_n * (1 - smoother_ratio)
    SMOOTHER_N = 3
    SMOOTHER_RATIO = .9
    
    def __init__(
            self, 
            smoothing_days=SMOOTHING_DAYS, 
            lag_days=LAG_DAYS, 
            neighor_radius_miles=NEIGHBOR_RADIUS_MILES, 
            neighbor_count_thresh=NEIGHBOR_COUNT_THRESH, 
            overwrite=False):
        """
        This class computes a nominal production for each home.  This is basically a smoothed
        daily production where higher production values are weighted more heavily than lower.
        This is important because many things can cause production to dip, but almost nothing can
        cause it to increase.  So nominal production is a proxy for "max production" over a time window.

        Args:
                   smoothing_days: The number of historical days to use in running the rolling weighted smoother
                                   to determine nominal production
                         lag_days: The number of days to use for computimg the log production derivative
            neighbor_radius_miles: Neighbors within this radius will be searched to see if muting is required
            neighbor_count_thresh: If this many neighbors also have detections, than this detection is muted.
        """
        self.smoothing_days = smoothing_days
        self.lag_days = lag_days
        self.neighor_radius_miles = neighor_radius_miles
        self.neighbor_count_thresh = neighbor_count_thresh
        
        self.earliest_start_date = EARLIEST_DATE
        
        # Compute alpha/beta parameters for smoothing distribution
        ratio = self.SMOOTHER_RATIO  # this is like win probability
        N = self.SMOOTHER_N   # This is like, total number of bernoulli tries
        a = self.SMOOTHER_N * self.SMOOTHER_RATIO
        b = N - a
        
        # Create the smoothing distribution
        self.smoothing_dist = stats.beta(a + 1, b + 1)
        
    def get_raw_production_for_home(self, homeowner_id, starting=None):
        with get_connections(LOCAL_CONN_NAME) as conn:
            hist = conn.table('prod_history')
            hist = hist[hist.homeowner_id == homeowner_id]
            hist = hist['date', 'total_production']
            if starting is not None:
                hist = hist[hist.date >= starting]
            df = hist.execute().set_index('date')
        return df
    
    def _rank_weighted_smoother(self, ser):
        """
        This function performs rank-based smoothing.  It is intended to be mapped
        over a time series with no gaps
        """
        # The raw weights are just beta transformations of the percent rank
        w = ser.rank(pct=True)
        w = self.smoothing_dist.pdf(w) 

        # Normalize the weights
        w = w / np.sum(w)

        # The sum of values * normalized weights is just the weighted mean
        return np.sum(ser.values * w)
    
    def _curtail_small_history(self, df, smoothing_days):
        has_enough = True
        if len(df) <= 2 * smoothing_days:
            has_enough = False
            df = pd.DataFrame(columns=df.columns)
        
        return df, has_enough

    def get_nominal_production_for_home(self, homeowner_id, starting=None):
        """
        Uses a rank-weighted smoothing algorithm to come up with nominal production
        and potential detections
        """
        # Get all prodution for this home
        df = self.get_raw_production_for_home(homeowner_id, starting)
        
        df, has_enough = self._curtail_small_history(df, self.smoothing_days)
        if not has_enough:
            return pd.DataFrame(columns=df.columns)
        
        # Make sure there are production values for every day (fill in zeros for missing days
        df = df.resample('D').asfreq()
        df['total_production'] = df.total_production.fillna(0)
        
        # Apply the rank-weighted smoothing to obtain nominal production
        df['nominal_prod'] = df['total_production'].rolling(self.smoothing_days).apply(self._rank_weighted_smoother)
        
        # You want to compute something like the d/dt(log(nominal_production)) over some number of lagged days
        df['baseline_nominal_prod'] = (df.nominal_prod).shift(self.lag_days)   
        
        # The smoothing and differentiating create NaNs at boundaries, so kill those
        df = df.dropna()
        
        return df
    
    def update_nominal_prod(self, show_progress_bar=False):
        # Get the start date and only proceed if it's valid
        start_date = get_start_date(LOCAL_CONN_NAME, self.NOMINAL_PROD_TABLE_NAME)
        if start_date is None:
            return
        
        # When computing nominal prod, I'm going to need historical production
        # prior to the requested start date from smoothing and differencing.
        # This computes how many historical days I need
        days_prior = self.lag_days + 3 * self.smoothing_days
            
        # Get the actual start date I need to grab from production
        prod_start_date = start_date - relativedelta(days=days_prior)
        
        # Get a list of unique homes that had production since the prod start date
        unique_homes = get_unique_homes(prod_start_date)
        
        # If you want to show progress bar, wrap in tqdm
        if show_progress_bar:
            import tqdm.notebook as tqdm
            unique_homes = tqdm.tqdm(unique_homes)
        
        # Loop over all producing homes
        for homeowner_id in unique_homes:
            # Get the production for that home since the start date
            df = self.get_nominal_production_for_home(homeowner_id, prod_start_date)
            
            # I only care about records that need to be inserted
            df = df.loc[start_date:, :].reset_index()
            
            # Only do something if there are records to insert
            if not df.empty:
                # Tag the frame with the homeowner_id and push it to destination table
                df.insert(0, 'homeowner_id', homeowner_id)
                with get_connections(LOCAL_CONN_NAME) as conn:
                    conn.insert(NOMINAL_PROD_TABLE_NAME, df)



class Detector(ezr.pickle_cache_mixin):

    def __init__(
            self, 
            smoothing_days=14, 
            lag_days=14, 
            slope_ratio_threshold=.6, 
            neighor_radius_miles=50, 
            neighbor_count_thresh=4, 
            overwrite=False):
        """
        This class essentially takes the derivative of log(production) by doing a "finite difference"
        over a several day time-frame.  If nominal production drops by a specified amount over a given
        timeframe, a potential detection is flagged.  Then nearby homes are checked for detections.  If
        they are also flagged, it's probably a widespread event not associated with this home, so the
        detection is muted.
        """
        self.smoothing_days = smoothing_days
        self.lag_days = lag_days
        self.slope_ratio_threshold = slope_ratio_threshold
        self.neighor_radius_miles = neighor_radius_miles
        self.neighbor_count_thresh = neighbor_count_thresh
        
        self.earliest_start_date = pd.Timestamp('1/1/2020')
        
    def extract_detections_from_nonimal_prod(self, df, slope_ratio_threshold):
        """
        Gets all potential detections from a dataframe of nominal productions.
        df: A dataframe indexed on time (with no gaps).
            must have cols: nominal_prod, baseline_nominal_prod
                baseline_nominal_prod is just the lagged nominal_production we use as a baseline
        """
        # Here is where you compute potential detections. "Did the production drop by XX percent over lag days?"        
        df['is_below_thresh'] = (df.nominal_prod < slope_ratio_threshold * df.baseline_nominal_prod).astype(int)
        
        
        
        # Find all days where potential detections fire
        df['raw_detection'] = np.maximum(df.is_below_thresh.diff(), 0)

        # Return all potential detections
        df = df[df.raw_detection > 0].reset_index()
        return df
        
    def get_raw_detections_for_home(self, homeowner_id, start_date):
        """
        Find all raw detections for a specific home given detector parameters
        """
        with get_connections(LOCAL_CONN_NAME) as conn: 
            nominal_prod = conn.table(NOMINAL_PROD_TABLE_NAME)
            nominal_prod = nominal_prod[nominal_prod.homeowner_id == homeowner_id]
            nominal_prod[nominal_prod.date >= start_date]
            df = nominal_prod.execute()
        
        # Extract the raw detections
        df = self.extract_detections_from_nonimal_prod(df, self.slope_ratio_threshold)
        
        # Add meta info
        df['homeowner_id'] = homeowner_id
        df['lag_days'] = self.lag_days
        df['detection_ratio'] = self.slope_ratio_threshold
        
        df = df[df.date >= start_date]
        
        # Return the frame
        df = df[['homeowner_id', 'date', 'total_production', 'nominal_prod', 'baseline_nominal_prod', 'lag_days', 'detection_ratio']]
        return df
        
    
    def compute_raw_detections(self, show_progress_bar=False):
        start_date = get_start_date(LOCAL_CONN_NAME, RAW_DETECTION_TABLE_NAME)
        if start_date is None:
            return self
        
        homeowner_ids = get_unique_homes(start_date)

        # If you want to show progress bar, wrap in tqdm
        if show_progress_bar:
            import tqdm.notebook as tqdm
            homeowner_ids = tqdm.tqdm(homeowner_ids)
            
        
        for homeowner_id in homeowner_ids:
            df = self.get_raw_detections_for_home(homeowner_id, start_date)
            if not df.empty:
                with get_connections(LOCAL_CONN_NAME) as conn:
                    conn.insert(RAW_DETECTION_TABLE_NAME, df)
        return self

                
    def _get_neighbor_counts(self, neighbors, observed, max_distance_miles=50, count_field_name=None):
        """
        Ibis logic to get the number of observed neighbors from a table of observations
        """
        # Get only the relevant fields from the observed table
        observed = observed['homeowner_id', 'date']

        # Limit neighbors to specified distance
        neighbors = neighbors[neighbors.distance_miles <= max_distance_miles].drop('distance_miles')

        # Join up each observation with all its neighbors
        observed_with_neighbors = observed.left_join(neighbors, [('homeowner_id', 'homeowner_id1')]).drop('homeowner_id1')

        # Join neighbors up with observations
        observed_with_observed_neighbors = observed_with_neighbors.left_join(observed, [(observed_with_neighbors.homeowner_id2, observed.homeowner_id), (observed_with_neighbors.date, observed.date)])

        # Record whether or not neighbors were observed
        observed_with_observed_neighbors = observed_with_observed_neighbors.mutate(observed=_.homeowner_id_y.notnull().cast('int8'))

        # Clean up the join field names
        observed_with_observed_neighbors = observed_with_observed_neighbors[[c for c in observed_with_observed_neighbors.columns if '_y' not in c]]
        observed_with_observed_neighbors = observed_with_observed_neighbors.relabel({c:c.replace('_x', '') for c in observed_with_observed_neighbors.columns})

        # Count the number of observed neighbors for each home
        observed_with_observed_neighbor_counts = observed_with_observed_neighbors.group_by(['homeowner_id', 'date']).aggregate(num_observed=_.observed.sum())

        # Rename the output field if desired
        if count_field_name is not None:
            observed_with_observed_neighbor_counts = observed_with_observed_neighbor_counts.relabel({'num_observed': count_field_name})

        return observed_with_observed_neighbor_counts
    
    def compute_detections(self):
        # We only need to compute detections that haven't already been computed
        start_date = get_start_date(LOCAL_CONN_NAME, DETECTION_TABLE_NAME)
        if start_date is None:
            return self
        
        # Get a connection to the target db
        with get_connections(LOCAL_CONN_NAME) as conn:
            # Get a table of all raw connections
            raw_detections = conn.table('raw_detections')

            # Get the table of neighbors
            neighbors = conn.table('neighbors')

            # Get a count of detected neighbors
            detection_neighbor_counts = self._get_neighbor_counts(neighbors, raw_detections, count_field_name='num_detected_neighbors')

            # Update raw detections with counts of detected neighbors
            raw_detections = raw_detections.inner_join(
                detection_neighbor_counts, 
                [
                    (raw_detections.homeowner_id == detection_neighbor_counts.homeowner_id), 
                    (raw_detections.date == detection_neighbor_counts.date), 
                ]
            )
            detections = raw_detections[raw_detections.num_detected_neighbors < self.neighbor_count_thresh]
            detections = detections[[c for c in detections.columns if not c.endswith('_y')]]
            detections = detections.relabel({c: c.replace('_x', '') for c in detections.columns})

            detections = detections[detections.date >= start_date]

            # Get a dataframe of detections
            dfd = detections.execute()
            if not dfd.empty:
                # Now save the detections to the duck database
                conn.insert('detections', dfd)
        return self

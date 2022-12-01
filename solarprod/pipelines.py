from data_plumbing import (
    sync_homeowners,
    sync_prod_history,
    update_neighbors,
    push_detections,
)

from detector_lib import (
    NominalProd,
    Detector,
)

from utils import logged


def run_detector_pipeline(memory_friendly=True, show_progress_bar=False):
    """
    Syncs all data required to look for detections.
    Computes detections.
    Pushes detections to destination
    """
    with logged('sync_homeowners'):
        sync_homeowners()

    with logged('sync_production'):
        sync_prod_history(show_progress_bar, memory_friendly)

    with logged('update_neighbors'):
        update_neighbors()
    
    with logged('update_nominal_prod'):
        NominalProd().update_nominal_prod(show_progress_bar)
    
    with logged('update_detections'):
        (
            Detector()
            .compute_raw_detections(show_progress_bar)
            .compute_detections()
        )

    with logged('push_detections'):
        push_detections()
                
run_detector_pipeline(show_progress_bar=True)                
                
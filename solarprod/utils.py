import contextlib
import easier as ezr

@contextlib.contextmanager
def logged(tag):
    logger = ezr.get_logger(tag)
    logger.info(f'{tag}: starting')
    yield
    logger.info(f'{tag}: complete')
    
    

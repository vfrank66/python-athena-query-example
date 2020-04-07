import time 
import logging
from logging import getLogger

logging.basicConfig(level=logging.INFO)
logger = getLogger(__name__)

def timeit(_func=None, *, msg=None):
    """A timer decorator for methods includes name, args, total min,
    and additional msg to print out to the console
    """
    def decorator_timed(method):
        def timed(*args, **kw):
            time_start = time.time()
            result = method(*args, **kw)
            time_end = time.time()
            time_msg = str(round((time_end - time_start), 1))
            if msg:
                logger.info(f'{method.__name__} {msg} - {time_msg} sec')
            else:
                logger.info(f'{method.__name__} - {time_msg} sec')
            return result

        return timed

    if _func is None:
        return decorator_timed
    else:
        return decorator_timed(_func)

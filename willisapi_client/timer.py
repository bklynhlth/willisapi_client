from functools import wraps
from time import time

from willisapi_client.logging_setup import logger as logger

def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(time() * 1000)) - start
            end_ = end_ if end_ > 0 else 0
            con_sec, con_min, con_hour = convertMillis(end_)
            logger.info(f"total time taken: {con_hour:02d}:{con_min:02d}:{con_sec:02d}")
    return _time_it

def convertMillis(millis):
    seconds=int(millis/1000)%60
    minutes=int(millis/(1000*60))%60
    hours=int(millis/(1000*60*60))%24
    return seconds, minutes, hours
 

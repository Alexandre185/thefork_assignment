import logging
import time
from functools import wraps


logger = logging.getLogger(__name__)


def timed(func):
    """
    This decorator logs the execution time for the decorated function

    :param func: function which execution time is logged
    :return: function wrapper that logs its execution time
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info("{} ran in {}s".format(func.__name__, round(end - start, 5)))

        return result

    return wrapper

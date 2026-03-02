""" Decorrators functions for kedro functions on nodes """

import logging as log
from functools import wraps
from time import time, sleep
import logging
import signal
import pandas as pd


def timing(f):
    """Python decorator to measure the time spent in the decorated function"""

    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logging.info(f"func:{f.__name__} time: {round(te-ts,2)} sec")
        return result

    return wrap


def repeat_if_fail(num_retries: int, time_between_retries: int, default_return: any):
    """Python decorator to repeat the funcion if fail

    :param num_retries: Number of times the function is executed
    :type num_retries: int
    :param time_between_retries: Time between execute the function
    :type time_between_retries: int
    :param default_return: Value to return if all tries fails
    :type default_return: any

    :Example:
    >>> @repeat_if_fail(num_retries=2, time_between_retries=1, default_return=pd.DataFrame())
    >>> def failing_func(i):
    >>>     if i%2==0:
    >>>         raise Exception(f"error {i}")
    >>>     else:
    >>>         return i
    >>> for i in range(2):
    >>>     print(failing_func(i))

    ERROR:root:func:failing_func ex: error 0
    ERROR:root:error 0
    Traceback (most recent call last):
      File "/tmp/ipykernel_28808/4058882032.py", line 11, in wrapper
        result = func(*args, **kwargs)
      File "/tmp/ipykernel_28808/1014737039.py", line 4, in failing_func
        raise Exception(f"error {i}")
    Exception: error 0
    ERROR:root:func:failing_func ex: error 0
    ERROR:root:error 0
    Traceback (most recent call last):
      File "/tmp/ipykernel_28808/4058882032.py", line 11, in wrapper
        result = func(*args, **kwargs)
      File "/tmp/ipykernel_28808/1014737039.py", line 4, in failing_func
        raise Exception(f"error {i}")
    Exception: error 0
    Empty DataFrame
    Columns: []
    Index: []
    1
    """

    def repeat(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(num_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    logging.error(f"func:{func.__name__} ex: {e}")
                    logging.exception(e)
                    sleep(time_between_retries)
            return default_return

        return wrapper

    return repeat


def timeout_decorate(seconds: int, raise_error_content: str = "Timeout"):
    """Decorator to wrap a function with a timer. If the time is end the function is stopped and an exception is raised.
    Util to decorate functions that can block the execution and we can discard some cases.

    :param seconds: Number of seconds we want to wait until raise the exception
    :type seconds: int
    :param raise_error_content: Text that we want to put in the exception, defaults to "Timeout"
    :type raise_error_content: str, optional
    """

    def timeout(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def signal_handler():
                raise TimeoutError(raise_error_content)

            signal.signal(signal.SIGALRM, signal_handler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)

            return result

        return wrapper

    return timeout


def features_length(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract the "features" dataframe from the arguments
        features = None
        try:
            features = args[0]
        except:
            logging.info('No features dataframe found in the arguments.')

        # Call the decorated function and capture its output
        result = func(*args, **kwargs)

        # Print the lengths of the input and output dataframes
        if isinstance(features, pd.DataFrame):
            logging.info(f'Input features length: {len(features)}')
        if isinstance(result, pd.DataFrame):
            logging.info(f'Output features length: {len(result)}')

        return result

    return wrapper

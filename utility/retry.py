import time
import logging

logger = logging.getLogger(__name__)


def retry_until_timeout(func, timeout=300, interval=5, *args, **kwargs):
    """
    Retry the function until it returns True or the timeout is reached.

    Args:
        func (func): The function to retry.
        timeout (int): The maximum time to retry the function.
        interval (int): The time between retries.

    Returns:
        Any: The return value of the function.

    Raises:
        Any exception: raise the exception the function last raised.

    """
    start_time = time.time()
    elapsed_time = time.time() - start_time
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{func.__name__} failed with exception: {e}")
            if elapsed_time > timeout:
                logger.error(f"{func.__name__} failed after {timeout} seconds")
                raise e
            else:
                elapsed_time = time.time() - start_time
                time_left = timeout - elapsed_time
                logger.info(
                    f"Retrying {func.__name__} in {interval} seconds: {time_left} seconds until timeout"
                )
                time.sleep(interval)


def retry_number_of_times(func, retries=3, interval=5, *args, **kwargs):
    """
    Retry the function a number of times.

    Args:
        func (func): The function to retry.
        retries (int): The number of times to retry the function.
        interval (int): The time between retries.

    Returns:
        Any: The return value of the function.

    Raises:
        Any exception: raise the exception the function last raised.

    """
    import time

    for attempt_num in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{func.__name__} failed with exception: {e}")
            if attempt_num == retries - 1:
                logger.error(f"{func.__name__} failed after {retries} attempts")
                raise e
            else:
                logger.info(
                    f"Retrying {func.__name__} in {interval} seconds: {retries - attempt_num - 1} attempts left"
                )
                time.sleep(interval)

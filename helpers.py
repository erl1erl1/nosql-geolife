import time

def time_elapsed_str(start_time):
    """
    Calculate the time elapsed from the start_time to now.

    :param start_time: The start time to calculate elapsed time from.
    :return: A string representing the elapsed time in minutes and seconds.
    """
    elapsed = time.time() - start_time
    minutes = round(elapsed / 60, 0)
    seconds = round(elapsed % 60, 0)
    return f'{minutes} minutes and {seconds} seconds.'
"""
Utitlity functions for running ray tasks etc...

CONTENTS
--------
get_futures_batch : function
    Throttle the return of ray futures by batching to regulate memory usage
"""

# Import dependencies
import ray

# Functions
def get_futures_batch(
    futures,
    batch_size=None,
    clr_refs=False, 
    verbose=False
):
    """Throttle the return of ray futures by batching to regulate memory usage
    """
    # if no batch size is specified, return the full list at once
    if batch_size is None:
        batch_size = len(futures)

    # calculate number of batches
    n_batches = int(len(futures)/batch_size)

    # batch get futures
    result = []
    for i in range(n_batches):
        chunk = futures[:batch_size]
        futures = futures[batch_size:]
        result.append(ray.get(chunk))
        if clr_refs:
            del chunk

    # delete futures object
    if len(futures) == 0:
        del futures
    return result
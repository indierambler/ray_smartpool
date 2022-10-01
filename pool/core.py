"""
Wrappers for native ray functions

CONTENTS
--------
get : function
    Throttle the return of ray futures by batching to regulate memory usage
"""

# Import dependencies
import time
import ray

# Functions
def get(
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


def get_eager(futures,delay=0.0):
    """Return ray futures as they are ready
    """
    results = []
    while True:
        time.sleep(delay)
        ready, not_ready = ray.wait(futures)
        if ready:
            results.append(ray.get(ready))
        futures = not_ready
        if not not_ready:
            break
    return results
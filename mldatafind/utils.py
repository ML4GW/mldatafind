from typing import TYPE_CHECKING, Any, Dict

import numpy as np
import psutil

if TYPE_CHECKING:
    from concurrent.futures import Future


BITS_PER_BYTE = 8


def _available_memory():
    """Returns current available RAM in GB"""
    return psutil.virtual_memory().available / (1024**3)


def _estimate_memory(
    n_channels: int,
    duration: float,
    precision: int = 64,
    sample_rate: float = 16384.0,
):
    """
    Estimate memory consumption of timeseries in GB
    """
    n_samples = n_channels * duration * sample_rate
    num_bytes = n_samples * (precision / BITS_PER_BYTE)
    num_gb = num_bytes / (1024**3)
    return num_gb


def _handle_future(future: "Future"):
    """Raise exception if future failed
    otherwise return its results
    """
    exc = future.exception()
    if exc is not None:
        raise exc
    return future.result()


def wait(futures: Dict[Any, "Future"]):
    """
    Extension of `concurrent.futures.wait` that returns key and future of dict
    """

    keys = np.array(list(futures.keys()))
    values = np.array(list(futures.values()))
    while True:

        status = [f.done() for f in values]

        if any(status):

            keys = keys[status]
            done = [futures.pop(key) for key in keys]

            return keys, done

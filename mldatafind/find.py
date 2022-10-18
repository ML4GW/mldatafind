from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional

from gwpy.segments import Segment, SegmentList

from mldatafind.io import fetch_timeseries, read_timeseries
from mldatafind.segments import query_segments

MEMORY_LIMIT = 1e10  # ? in bytes
BITS_PER_BYTE = 8


def _calc_memory(
    n_channels: int,
    duration: float,
    precision: int = 64,
    sample_rate: float = 16384.0,
):

    n_samples = n_channels * duration * sample_rate
    num_bytes = n_samples * (precision / BITS_PER_BYTE)
    return num_bytes


def _data_generator(
    segments: List,
    channels: Iterable[str],
    method: Callable,
    n_workers: int,
    thread: bool,
    **method_kwargs,
) -> Iterator:

    memory_limit = MEMORY_LIMIT

    if thread:
        executor = ThreadPoolExecutor(n_workers)
    else:
        executor = ProcessPoolExecutor(n_workers)

    with executor as exc:
        # keep track of current memory
        # and number of futures currently running
        current_memory = 0
        futures = []

        # while there are still futures or segments to analyze
        while segments or futures:

            # submit jobs until memory limit is reached
            while current_memory < memory_limit and segments:
                segment = segments.pop()
                duration = segment[1] - segment[0]

                # TODO: memory depends on sample rate;
                # for strain channels this is typically 16khz,
                # but unknown for auxiliary channels
                segment_memory = _calc_memory(len(channels), duration)

                future = exc.submit(
                    method, channels, *segment, **method_kwargs
                )
                futures.append(future)
                current_memory += segment_memory

            # memory limit is saturated:
            # wait until any one future completes and yield
            ready, futures = wait(futures, return_when=FIRST_COMPLETED)

            yield from ready


def find_data(
    t0: float,
    tf: float,
    channels: Iterable[str],
    sample_rate: float,
    min_duration: float = 0.0,
    segment_names: Optional[Iterable[str]] = None,
    data_dir: Optional[Path] = None,
    array_like: bool = False,
    n_workers: int = 4,
    thread: bool = True,
) -> Iterator:

    """
    Find gravitational wave data from `channels` over
    requested period `t0` to `tf`, yielding TimeSeriesDict's of the data
    for each segment.

    If `segment_names` is None, will use the entire duration
    from `t0` to `tf` as the only segment.
    Otherwise, corresponding segments / data quality flags will be queried
    via DataQualityDict.query_dqsegdb, and their intersection will be used.
    Only segments of length greater than `min_duration` will be yielded.


    If `data_dir` is specified, will read in the segments of data from h5 files
    following the f`{prefix}_{t0}_{duration}.h5` syntax. These files must
    contain h5 datasets with names corresponding to the requested channels.

    Args:
        t0: Beginning of requested period
        tf: End of requested period
        channels: Iterable of channel names to find
        min_duration: minimum duration to yield segments
        segment_names: Iterable of segment names for querying from dq_segdb
        data_dir: If specified, will read data from h5 files located here.

    Returns:
        Generator of TimeSeriesDict's for each segment of data

    """

    length = tf - t0
    if min_duration > length:
        raise ValueError(
            f"Minimum duration ({min_duration} s ) is longer than"
            f"requested analysis interval ({length} s)"
        )

    # if segment names are passed
    # query all those segments
    if segment_names is not None:
        segments = query_segments(segment_names, t0, tf, min_duration)
    else:
        segments = SegmentList(Segment([t0, tf]))

    # if no data dir has
    # been passed query via gwpy,
    # otherwise load from
    # directory
    method = (
        fetch_timeseries
        if data_dir is not None
        else partial(read_timeseries, data_dir)
    )
    _data_generator(
        segments, channels, method, n_workers, thread, array_like=array_like
    )

import logging
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, List, Optional

if TYPE_CHECKING:
    from concurrent.futures import Future

from gwpy.segments import Segment, SegmentList

import mldatafind.utils as utils
from mldatafind.io import fetch_timeseries, read_timeseries
from mldatafind.segments import query_segments

DEFAULT_SEGMENT_SERVER = os.getenv(
    "DEFAULT_SEGMENT_SERVER", "https://segments.ligo.org"
)

MEMORY_LIMIT = 5  # GB


def _handle_future(future: "Future"):
    """Raise exception if future failed
    otherwise return its results
    """
    exc = future.exception()
    if exc is not None:
        raise exc
    return future.result()


def _data_generator(
    method: Callable,
    segments: List,
    channels: Iterable[str],
    n_workers: int,
    thread: bool,
    **method_kwargs,
) -> Iterator:

    if thread:
        executor = ThreadPoolExecutor(n_workers)
    else:
        executor = ProcessPoolExecutor(n_workers)

    logging.info(f"Finding {len(segments)} segments")

    with executor as exc:
        # keep track of current memory
        # and number of futures currently running
        current_memory = 0
        futures = {}

        # while there are still futures or segments to analyze
        while segments or futures:

            # submit jobs until memory limit is reached
            while current_memory < MEMORY_LIMIT and segments:
                segment = segments.pop()

                duration = segment[1] - segment[0]

                # TODO: memory depends on sample rate;
                # for strain channels this is typically 16khz,
                # but unknown for auxiliary channels
                segment_memory = utils._estimate_memory(
                    len(channels), duration
                )

                future = exc.submit(
                    method, channels, *segment, **method_kwargs
                )
                logging.info(
                    f"Future submitted to query {duration} s of data"
                    f"and {segment_memory:.2f} GB of memory"
                )
                futures[segment_memory] = future
                current_memory += segment_memory

            # memory limit is saturated:
            # wait until any one future completes and yield
            memories, done = utils.wait(futures)

            for memory, f in zip(memories, done):
                result = _handle_future(f)
                yield result
                current_memory -= memory


def find_data(
    t0: float,
    tf: float,
    channels: Iterable[str],
    min_duration: float = 0.0,
    segment_names: Optional[Iterable[str]] = None,
    data_dir: Optional[Path] = None,
    array_like: bool = False,
    n_workers: int = 4,
    thread: bool = True,
    segment_url: str = DEFAULT_SEGMENT_SERVER,
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
        segments = query_segments(
            segment_names, t0, tf, min_duration, segment_url=segment_url
        )
    else:
        segments = SegmentList(Segment([t0, tf]))

    # if no data dir has been passed query via gwpy,
    # otherwise load from specified directory
    method = (
        fetch_timeseries
        if data_dir is None
        else partial(read_timeseries, data_dir)
    )

    segments = [list(segment) for segment in segments]
    return _data_generator(
        method, segments, channels, n_workers, thread, array_like=array_like
    )

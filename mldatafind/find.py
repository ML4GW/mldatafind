import logging
import os
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Tuple

from gwpy.segments import Segment, SegmentList

import mldatafind.utils as utils
from mldatafind.io import fetch_timeseries, read_timeseries
from mldatafind.segments import query_segments

DEFAULT_SEGMENT_SERVER = os.getenv(
    "DEFAULT_SEGMENT_SERVER", "https://segments.ligo.org"
)

MEMORY_LIMIT = 5  # GB


@dataclass(frozen=True)
class Loader:
    data_dir: Optional[Path] = None
    array_like: bool = False

    def __call__(self, channels: List[str], start: float, stop: float):
        if self.data_dir is not None:
            return read_timeseries(
                self.data_dir, channels, start, stop, self.array_like
            )
        else:
            return fetch_timeseries(
                channels, start, stop, array_like=self.array_like
            )


def data_generator(
    exc,
    segments: List[Tuple[float, float]],
    loader: Loader,
    channels: List[str],
    chunk_size: Optional[float] = None,
    current_memory: float = 0,
    retain_order: bool = False,
):
    # if we're chunking, we don't need to keep track
    # of memory since we'll leave that to the generators
    # that we'll create. If we're not, we have to keep
    # track of the memory demanded by each load so that
    # we can know how much each future will free once it
    # gets processed (and presumably deleted)
    if chunk_size is not None:
        futures, rm_futures = [], []
    else:
        futures = OrderedDict()

    def maybe_submit(current_memory, return_value=None):
        # if we passed a future and we're not chunking,
        # this means we're about to yield it, so subtract
        # its memory footprint from our running total
        if return_value is not None and chunk_size is None:
            memory = futures.pop(return_value)
            current_memory -= memory
            return_value = return_value.result()
        else:
            rm_futures.append(return_value)

        # if our memory is currently full or we have no
        # more segments to submit for loading, then
        # short-circuit here
        if current_memory > MEMORY_LIMIT or not segments:
            return return_value

        # check the next segment to see if we can load it
        start, stop = segments.pop()
        duration = stop - start

        # if we're chunking our segments, return a
        # generator of segments rather than
        if chunk_size is not None:
            if duration > chunk_size:
                num_segments = (duration - 1) // chunk_size + 1
                segs = []
                for i in range(num_segments):
                    end = min(start + (i + 1) * chunk_size, stop)
                    seg = (start + i * chunk_size, end)
                    segs.append(seg)
            else:
                segs = [seg]

            # call this function recursively but with
            # chunking turned off since we know that
            # all the segments will have the right length
            gen = data_generator(
                exc,
                segs,
                loader,
                chunk_size=None,
                current_memory=current_memory,
                retain_order=True,
            )
            futures.append(gen)
            return return_value or gen

        # if we're not chunking, submit this segment for loading
        # TODO: should we check if this is going to put us over?
        # The downside is that we'll never be over, and so the
        # check at the top will have to be adjusted.
        mem = utils._estimate_memory(len(channels), duration)
        future = exc.submit(loader, channels, start, stop)
        logging.debug(
            "Submitted future to query {}s of data "
            "and {:0.2f}GB of memory".format(duration, mem)
        )

        # record its memory footprint and future
        current_memory += mem
        futures[future] = mem
        return return_value or future

    while segments or futures:
        # submit as many jobs as we can up front
        while True:
            if maybe_submit(current_memory, None) is None:
                break

        # if we're chunking, then return generators
        # as they become available
        if chunk_size is not None:
            for generator in futures:
                yield maybe_submit(current_memory, generator)

            # now get rid of any generators that have been yielded
            for future in rm_futures:
                futures.pop(future)
            rm_futures = []
        elif retain_order:
            # if we're retaining order, break as
            # soon as we encounter a future that
            # hasn't completed because we don't
            # care if any ones after it have. Don't
            # iterate through the dict in-place
            fs = list(futures.keys())
            for future in fs:
                if not future.done():
                    break
                yield maybe_submit(current_memory, future)
        else:
            done, _ = wait(futures.keys(), timeout=1e-3)
            for f in done:
                yield maybe_submit(current_memory, future)


def find_data(
    t0: float,
    tf: float,
    channels: Iterable[str],
    min_duration: float = 0.0,
    segment_names: Optional[Iterable[str]] = None,
    chunk_size: Optional[float] = None,
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
    loader = Loader(data_dir, array_like)
    segments = [tuple(segment) for segment in segments]

    exc_type = ThreadPoolExecutor if thread else ProcessPoolExecutor
    with exc_type(n_workers) as exc:
        return data_generator(
            exc,
            segments,
            loader,
            channels,
            chunk_size=chunk_size,
            current_memory=0,
        )

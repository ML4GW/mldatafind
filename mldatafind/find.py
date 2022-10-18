from collections import defaultdict
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional

import numpy as np
from gwpy.segments import DataQualityDict, Segment, SegmentList
from gwpy.timeseries import TimeSeries, TimeSeriesDict

from mldatafind.io import filter_and_sort_files, read_timeseries

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


def fetch(
    channels: List[str],
    t0: float,
    tf: float,
    sample_rate: float,
    nproc: int = 1,
):
    ts_dict = TimeSeriesDict.get(channels, t0, tf, nproc=nproc)
    ts_dict = ts_dict.resample(sample_rate)
    return ts_dict


def read(
    data_dir: Path,
    channels: Iterable[str],
    t0: float,
    tf: float,
):

    # find and sort all files
    # that match file name convention
    matches = filter_and_sort_files(data_dir, return_matches=True)
    paths = np.array([data_dir / i.string for i in matches])

    # downselect to paths that contain requested data
    starts = np.array([float(match.group("t0")) for match in matches])
    stops = (
        np.array([float(match.group("length")) for match in matches]) + starts
    )

    mask = starts < tf
    mask &= stops > t0

    paths = paths[mask]

    outputs = defaultdict(lambda: np.array([]))
    times = []

    for path in paths:
        datasets, t = read_timeseries(path, *channels)
        for channel, dataset in datasets.items():
            dataset = np.append(outputs[channel], dataset)
            outputs[channel] = dataset

        if times:
            # check for contiguousness
            if t[0] != times[-1]:
                raise ValueError(
                    f"{data_dir} does not contain a contiguous stretch of"
                    f" data from {t0} to {tf}"
                )

        times.extend(t)

    ts_dict = TimeSeriesDict()
    for channel in channels:
        ts_dict[channel] = TimeSeries(outputs[channel], times=times)

    ts_dict = ts_dict.crop(t0, tf)
    return ts_dict


def query_segments(
    segment_names: Iterable[str], t0: float, tf: float, min_duration: float = 0
) -> SegmentList:
    """
    Query segments from dqsegdb and return the intersection.
    Only return segments of length greater than `min_duration`

    Args:
        segment_names: Iterable of segment names to query
        t0: Start time of segments
        tf: Stop time of segments
        min_duration: Minimum length of intersected segments

    Returns SegmentList
    """

    segments = DataQualityDict.query_dqsegdb(
        segment_names,
        t0,
        tf,
    )

    segments = np.array(segments.intersection().active.copy())

    # if min duration is passed, restrict to those segments
    mask = np.ones(len(segments), dtype=bool)

    if min_duration is not None:
        durations = np.array([float(seg[1] - seg[0]) for seg in segments])
        mask &= durations > min_duration

    segments = segments[mask]

    return segments


def _data_generator(
    segments: List,
    channels: Iterable[str],
    method: Callable,
    n_workers: int,
    thread: bool,
) -> Iterator[TimeSeriesDict]:

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

                future = exc.submit(method, channels, *segment)
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
    n_workers: int = 4,
    thread: bool = True,
) -> Iterator[TimeSeriesDict]:

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
    method = fetch if data_dir is not None else partial(read, data_dir)

    _data_generator(segments, channels, method, n_workers, thread)

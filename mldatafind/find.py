import logging
import os
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from mldatafind.io import fetch_timeseries, read_timeseries

DEFAULT_SEGMENT_SERVER = os.getenv(
    "DEFAULT_SEGMENT_SERVER", "https://segments.ligo.org"
)


BITS_PER_BYTE = 8
MEMORY_LIMIT = 5


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
    channels: Sequence[str],
    chunk_size: Optional[float] = None,
    current_memory: Optional[List[float]] = None,
    retain_order: bool = False,
):
    if current_memory is None:
        current_memory = [0]
    futures = OrderedDict()

    def maybe_submit(current_memory, return_value=None):
        # if we passed a future or generator to return,
        # remove it from our futures tracker
        if return_value is not None:
            memory = futures.pop(return_value)

            if memory is not None:
                # this means we're not chunking and so
                # this represents a future with some
                # corresponding amount of memory, so
                # subtract it from our tracker
                current_memory[0] -= memory
                return_value = return_value.result()

        # start submitting futures until we fill
        # up the hole we created in our memory limit
        while current_memory[0] <= MEMORY_LIMIT and segments:
            start, stop = segments.pop(0)
            duration = stop - start

            # if we're chunking, it only matters if the first
            # chunk will put us over the limit
            if chunk_size is not None:
                size = min(duration, chunk_size)
                mem = _estimate_memory(len(channels), size)
            else:
                mem = _estimate_memory(len(channels), duration)

            if (current_memory[0] + mem) > MEMORY_LIMIT:
                segments.insert(0, (start, stop))
                break

            # if we're chunking our segments, return a
            # generator of segments rather than
            if chunk_size is not None:
                if duration > chunk_size:
                    num_segments = int((duration - 1) // chunk_size) + 1
                    segs = []
                    for i in range(num_segments):
                        end = min(start + (i + 1) * chunk_size, stop)
                        seg = (start + i * chunk_size, end)
                        segs.append(seg)
                else:
                    segs = [(start, stop)]

                # call this function recursively but with
                # chunking turned off since we know that
                # all the segments will have the right length
                gen = data_generator(
                    exc,
                    segs,
                    loader,
                    channels,
                    chunk_size=None,
                    current_memory=current_memory,
                    retain_order=True,
                )
                futures[gen] = None
            else:
                # if we're not chunking, submit this segment for loading
                future = exc.submit(loader, channels, start, stop)
                logging.debug(
                    "Submitted future to query {}s of data "
                    "and {:0.2f}GB of memory".format(duration, mem)
                )

                # record its memory footprint and future
                current_memory[0] += mem
                futures[future] = mem

        return return_value

    while segments or futures:
        # submit as many jobs as we can up front
        maybe_submit(current_memory, None)
        if chunk_size is not None or retain_order:
            fs = list(futures.keys())
            if chunk_size is None:
                done = True
                fs = [f for f in fs if (done := done and f.done())]
        elif chunk_size is None:
            fs, _ = wait(futures.keys(), timeout=1e-3)

        for future in fs:
            yield maybe_submit(current_memory, future)


def find_data(
    segments: List[Tuple[float, float]],
    channels: Sequence[str],
    chunk_size: Optional[float] = None,
    data_dir: Optional[Path] = None,
    retain_order=False,
    array_like: bool = False,
    n_workers: int = 4,
    thread: bool = True,
    segment_url: str = DEFAULT_SEGMENT_SERVER,
) -> Iterator:

    """
    Find gravitational wave data from `channels` during segments corresponding
    to `segment_names` between  requested period `t0` to `tf`,
    yielding TimeSeriesDict's of the data for each active segment.

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
        retain_order:
            If True, segments will be returned in chronological order.
            Note that if a chunk_size is passed this is always True.
        array_like:
            If True, return data in numpy arrays. Otherwise, will return
            gwpy.Timeseries.TimeSeriesDict
        n_workers:
            Number of workers for Executor to use
        thread: If True, use thread pool, otherwise process pool
        segment_url: URL of database for querying segments

    Returns:
        Iterator of timeseries data for requested segments
    """

    if not retain_order and chunk_size:
        logging.warning(
            "When chunk size is passed segment order is always maintained"
        )

    # if no data dir has been passed query via gwpy,
    # otherwise load from specified directory
    loader = Loader(data_dir, array_like)
    segments = [tuple(segment) for segment in segments]

    exc_type = ThreadPoolExecutor if thread else ProcessPoolExecutor
    with exc_type(n_workers) as exc:
        yield from data_generator(
            exc,
            segments,
            loader,
            channels,
            chunk_size=chunk_size,
            current_memory=None,
            retain_order=retain_order,
        )

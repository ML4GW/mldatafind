from typing import Iterable

from gwpy.segments import DataQualityDict, DataQualityFlag, SegmentList

from mldatafind.authenticate import authenticate

# TODO: Not exactly sure what these flags
OPEN_DATA_FLAGS = ["H1_DATA", "L1_DATA", "V1_DATA"]


def _fetch_open_data(
    flags: Iterable[str], start: float, end: float, **kwargs
) -> DataQualityDict:
    dqdict = DataQualityDict()
    for flag in flags:
        dqdict[flag] = DataQualityFlag.fetch_open_data(
            flag, start, end, **kwargs
        )
    return dqdict


def query_segments(
    flags: Iterable[str],
    start: float,
    end: float,
    min_duration: float = 0,
    **kwargs,
) -> SegmentList:
    """
    Query segments from dqsegdb and return the intersection.
    Only return segments of length greater than `min_duration`

    Args:
        flags: Iterable of data flags to query
        start: Start time of segments
        end: End time of segments
        min_duration: Minimum length of intersected segments
        **kwargs: Keyword arguments to DataQualityDict.query_dqsegdb
    Returns SegmentList
    """

    length = end - start
    if min_duration > length:
        raise ValueError(
            f"Minimum duration ({min_duration} s) is longer than "
            f"requested analysis interval ({length} s)"
        )

    # split open data flags from private flags
    open_data_flags = list(filter(lambda x: x in OPEN_DATA_FLAGS, flags))
    flags = list(filter(lambda x: x not in OPEN_DATA_FLAGS, flags))
    
    segments = DataQualityDict()
    # only try to query private flags if there are any
    # otherwise we'll run into an error 
    if flags:
        try:
            segments = DataQualityDict.query_dqsegdb(
                flags,
                start,
                end,
                **kwargs,
            )
        except OSError as e:
            if not str(e).startswith("Could not find the TLS certificate file"):
                # TODO: what's the error for an expired certificate?
                raise

            # try to authenticate then re-query
            authenticate()
            segments = DataQualityDict.query_dqsegdb(
                flags,
                start,
                end,
                **kwargs,
            )

    # if open data flags are requested,
    # query them and combine with private flags
    if open_data_flags:
        open_data_segments = _fetch_open_data(
            open_data_flags, start, end, **kwargs
        )
        segments.update(open_data_segments)

    segments = segments.intersection().active
    if min_duration > 0:
        segments = filter(lambda i: i[1] - i[0] >= min_duration, segments)
        segments = SegmentList(segments)
    return segments

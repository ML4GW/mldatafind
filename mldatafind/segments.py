from typing import Iterable

import numpy as np
from gwpy.segments import DataQualityDict, SegmentList


def query_segments(
    segment_names: Iterable[str],
    t0: float,
    tf: float,
    min_duration: float = 0,
    **kwargs,
) -> SegmentList:
    """
    Query segments from dqsegdb and return the intersection.
    Only return segments of length greater than `min_duration`

    Args:
        segment_names: Iterable of segment names to query
        t0: Start time of segments
        tf: Stop time of segments
        min_duration: Minimum length of intersected segments
        **kwargs: Keyword arguments to DataQualityDict.query_dqsegdb
    Returns SegmentList
    """

    length = tf - t0
    if min_duration > length:
        raise ValueError(
            f"Minimum duration ({min_duration} s) is longer than "
            f"requested analysis interval ({length} s)"
        )

    segments = DataQualityDict.query_dqsegdb(
        segment_names,
        t0,
        tf,
        **kwargs,
    )

    segments = np.array(segments.intersection().active.copy())

    # if min duration is passed, restrict to those segments
    mask = np.ones(len(segments), dtype=bool)

    if min_duration is not None:
        durations = np.array([float(seg[1] - seg[0]) for seg in segments])
        mask &= durations > min_duration

    segments = segments[mask]

    return segments

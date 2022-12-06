from typing import Iterable

from gwpy.segments import DataQualityDict, SegmentList

from mldatafind.authenticate import authenticate


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

    try:
        segments = DataQualityDict.query_dqsegdb(
            segment_names,
            t0,
            tf,
            **kwargs,
        )
    except OSError as e:
        if not str(e).startswith("Could not find the TLS certificate file"):
            # TODO: what's the error for an expired certificate?
            raise

        # try to authenticate then re-query
        authenticate()
        segments = DataQualityDict.query_dqsegdb(
            segment_names,
            t0,
            tf,
            **kwargs,
        )

    segments = segments.intersection().active
    if min_duration is not None:
        segments = filter(lambda i: i[1] - i[0] >= min_duration, segments)
        segments = SegmentList(segments)
    return segments

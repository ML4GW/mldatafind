from unittest.mock import patch

import pytest
from gwpy.segments import (
    DataQualityDict,
    DataQualityFlag,
    Segment,
    SegmentList,
)

from mldatafind.segments import query_segments


def test_query_segments():

    # test simple example
    # where segments for both ifos
    # completely overlap
    segment_list = SegmentList(
        [
            Segment([0, 200]),
            Segment([1000, 1100]),
        ]
    )
    flags = [i + "1:ANALYSIS" for i in "HL"]

    segments = DataQualityDict(
        {flag: DataQualityFlag(active=segment_list) for flag in flags}
    )
    with patch(
        "mldatafind.segments.DataQualityDict.query_dqsegdb",
        return_value=segments,
    ):
        # technically the times don't matter since nothing
        # is actually getting queried but for clarity's sake
        intersection = query_segments(flags, 0, 1200)
        assert intersection == segment_list

        with pytest.raises(ValueError):
            query_segments(flags, 0, 1200, 1300)

        # now test with min duration argument
        # only first segment should be returned
        intersection = query_segments(flags, 0, 1200, min_duration=110)
        assert intersection == SegmentList([Segment([0, 200])])

    # now shift segments so that
    # there is no overlap
    for i, flag in enumerate(segments.values()):
        flag.active = segment_list.shift(i * 300)

    with patch(
        "mldatafind.segments.DataQualityDict.query_dqsegdb",
        return_value=segments,
    ):
        intersection = query_segments(flags, 0, 1200)
        assert len(intersection) == 0

    # TODO: test authentication behavior

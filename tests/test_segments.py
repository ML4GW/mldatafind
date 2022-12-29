from unittest.mock import call, patch

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
    open_flags = ["H1_DATA", "L1_DATA"]

    segments = DataQualityDict(
        {flag: DataQualityFlag(active=segment_list) for flag in flags}
    )

    with patch(
        "mldatafind.segments.DataQualityDict.query_dqsegdb",
        return_value=segments,
    ) as mock_query:
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

        with patch(
            "mldatafind.segments.DataQualityFlag.fetch_open_data",
            return_value=DataQualityFlag(active=segment_list),
        ) as mock_fetch:
            intersection = query_segments(
                flags + open_flags, 0, 1200, min_duration=110
            )

            assert intersection == SegmentList([Segment([0, 200])])
            mock_fetch.assert_has_calls(
                [call(flag, 0, 1200) for flag in open_flags]
            )

            # called 3 times above
            mock_query.assert_has_calls([call(flags, 0, 1200)] * 3)

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

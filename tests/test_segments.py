from unittest.mock import patch

import numpy as np
from gwpy.segments import (
    DataQualityDict,
    DataQualityFlag,
    Segment,
    SegmentList,
)

from mldatafind import io


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

    segments = DataQualityDict()
    for ifo in ["H1", "L1"]:
        segments[f"{ifo}:ANALYSIS"] = DataQualityFlag(active=segment_list)

    with patch(
        "mldatafind.io.DataQualityDict.query_dqsegdb", return_value=segments
    ):
        intersection = io.query_segments(
            ["H1:ANALYSIS", "L1:ANALYSIS"],
            -np.inf,
            np.inf,
        )

        assert (intersection == segment_list).all()

        # now test with min duration argument
        # only first segment should be returned
        intersection = io.query_segments(
            ["H1:ANALYSIS", "L1:ANALYSIS"],
            -np.inf,
            np.inf,
            min_duration=110,
        )

        assert (intersection == [[0, 200]]).all()

    # now shift segments so that
    # there is no overlap
    segments = DataQualityDict()
    for i, ifo in enumerate(["H1", "L1"]):
        segments[f"{ifo}:ANALYSIS"] = DataQualityFlag(
            active=segment_list.shift(i * 300)
        )

    with patch(
        "mldatafind.io.DataQualityDict.query_dqsegdb", return_value=segments
    ):
        intersection = io.query_segments(
            ["H1:ANALYSIS", "L1:ANALYSIS"],
            -np.inf,
            np.inf,
        )
        assert len(intersection) == 0

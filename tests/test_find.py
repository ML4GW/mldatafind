from unittest.mock import patch

import numpy as np
import pytest
from gwpy.segments import (
    DataQualityDict,
    DataQualityFlag,
    Segment,
    SegmentList,
)

import mldatafind.find as find


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
        "mldatafind.find.DataQualityDict.query_dqsegdb", return_value=segments
    ):
        intersection = find.query_segments(
            ["H1:ANALYSIS", "L1:ANALYSIS"],
            -np.inf,
            np.inf,
        )

        assert (intersection == segment_list).all()

        # now test with min duration argument
        # only first segment should be returned
        intersection = find.query_segments(
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
        "mldatafind.find.DataQualityDict.query_dqsegdb", return_value=segments
    ):
        intersection = find.query_segments(
            ["H1:ANALYSIS", "L1:ANALYSIS"],
            -np.inf,
            np.inf,
        )
        assert len(intersection) == 0


def test_read(
    create_test_data, sample_rate, file_length, prefix, channel_names, t0
):

    write_dir = create_test_data(sample_rate, file_length, prefix)

    read_duration = 100
    ts_dict = find.read(write_dir, channel_names, t0, t0 + read_duration)

    assert all([channel in ts_dict.keys() for channel in channel_names])
    assert ts_dict.span == (t0, t0 + read_duration)

    for i, (_, data) in enumerate(ts_dict.items()):
        assert (
            data.times.value
            == np.arange(t0, t0 + read_duration, 1 / sample_rate)
        ).all()
        assert (
            data.value
            == np.arange(0, read_duration * sample_rate, 1) * (i + 1)
        ).all()

    # test that value error raised
    # when trying to read channel
    # that doesn't exist
    with pytest.raises(ValueError) as err:
        ts_dict = find.read(
            write_dir, ["Wrong:ChannelName"], t0, t0 + read_duration
        )
        assert "channel" in err

    # test that value error raised
    # when trying to read beyond
    # duration of data that exists
    with pytest.raises(ValueError) as err:
        ts_dict = find.read(write_dir, channel_names, t0, t0 + 100000)
        assert "contigous" in err

    # test that value error raised
    # when trying to read from
    # discontiguous data

    with pytest.raises(ValueError) as err:
        ts_dict = find.read(write_dir, channel_names, t0 + 800, t0 + 990)
        assert "contigous" in err

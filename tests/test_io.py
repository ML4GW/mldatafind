from pathlib import Path
from unittest.mock import patch

import numpy as np
from gwpy.segments import (
    DataQualityDict,
    DataQualityFlag,
    Segment,
    SegmentList,
)

from mldatafind import io


def test_filter_and_sort_files(
    typed_file_names,
    path_type,
    file_names,
    t0,
    file_length,
    n_files,
):
    if isinstance(typed_file_names, (str, Path)):
        typed_file_names = Path(typed_file_names).parent
        typed_file_names = path_type(typed_file_names)
        expected_names = file_names
        expected_type = Path
    else:
        expected_names = file_names[: len(typed_file_names)]
        expected_names = list(map(path_type, expected_names))
        expected_type = path_type

    # test with passing just a string file
    # expect to return just this file
    result = io.filter_and_sort_files(file_names[0])

    assert len(result) == 1
    assert result == [file_names[0]]

    # test with passing just a path as file
    # expect to return just this file
    result = io.filter_and_sort_files(Path(file_names[0]))

    assert len(result) == 1
    assert result == [file_names[0]]

    result = io.filter_and_sort_files(typed_file_names)

    assert len(result) == len(expected_names)
    assert all([isinstance(i, expected_type) for i in result])
    assert all([i == j for i, j in zip(result, expected_names)])

    # now test with t0 and tf
    # such that only expect one file
    result = io.filter_and_sort_files(
        typed_file_names, t0=t0, tf=t0 + file_length - 1
    )
    assert len(result) == 1
    assert all([isinstance(i, expected_type) for i in result])

    # now test with t0 and tf
    # such that expect two files
    # (only run if number of files greater than 1)
    if n_files > 1:
        result = io.filter_and_sort_files(
            typed_file_names, t0=t0, tf=t0 + file_length + 1
        )
        assert len(result) == 2
        assert all([isinstance(i, expected_type) for i in result])

    # now test with t0 before start
    # such that all files should be returned
    result = io.filter_and_sort_files(
        typed_file_names,
        t0=t0 - 1,
    )
    print(result, expected_names)
    assert len(result) == n_files
    assert all([isinstance(i, expected_type) for i in result])
    assert all([i == j for i, j in zip(result, expected_names)])

    # now test with tf greater than
    # end of files such that all files should be returned
    tf = t0 + 1 + (n_files * file_length)
    result = io.filter_and_sort_files(typed_file_names, tf=tf)
    assert len(result) == n_files

    # now test with t0 greater than
    # end of files such that all files should be returned
    tf = t0 + 1 + (n_files * file_length)
    result = io.filter_and_sort_files(typed_file_names, t0=tf)
    assert len(result) == 0

    expected_names = [Path(i).name for i in expected_names]
    matches = io.filter_and_sort_files(typed_file_names, return_matches=True)
    assert len(matches) == len(expected_names)
    assert all([i.string == j for i, j in zip(matches, expected_names)])


def test_read_timeseries(
    file_names, t0, n_files, file_length, channel_names, sample_rate
):

    write_dir = file_names[0].parent

    # first try reading when passing
    # the write directory
    data, times = io.read_timeseries(write_dir, channel_names, t0, t0 + 1000)

    assert (times == np.arange(t0, t0 + 1000, 1 / sample_rate)).all()
    assert data.shape == (len(channel_names), sample_rate * 1000)
    for i, dataset in enumerate(data):
        assert (dataset == np.arange(0, 1000 * sample_rate) * (i + 1)).all()

    # now try reading when passing
    # list of files
    data, times = io.read_timeseries(file_names, channel_names, t0, t0 + 1000)

    for i, dataset in enumerate(data):
        assert (dataset == np.arange(0, 1000 * sample_rate) * (i + 1)).all()

    assert (times == np.arange(t0, t0 + 1000, 1 / sample_rate)).all()
    assert data.shape == (len(channel_names), sample_rate * 1000)

    # now try reading when passing
    # single file
    data, times = io.read_timeseries(
        file_names[0], channel_names, t0, t0 + file_length - 1
    )

    for i, dataset in enumerate(data):
        assert (
            dataset == np.arange(0, (file_length - 1) * sample_rate) * (i + 1)
        ).all()

    assert (
        times == np.arange(t0, t0 + file_length - 1, 1 / sample_rate)
    ).all()
    assert data.shape == (len(channel_names), sample_rate * (file_length - 1))


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

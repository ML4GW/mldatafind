from pathlib import Path
from unittest.mock import call, patch

import numpy as np
import pytest
from gwpy.timeseries import TimeSeries, TimeSeriesDict

from mldatafind import io


def check_file_contents(fname, sample_rate, t0, file_length, **datasets):

    # validate expectations using both gwpy
    # and our version of read_timeseries
    ts_dict = TimeSeriesDict.read(fname)

    for channel, dataset in datasets.items():
        ts = ts_dict[channel]
        assert (ts.value == dataset).all()
        assert ts.dt.value == 1 / sample_rate
        assert ts.t0.value == t0

        assert (
            ts.times.value == np.arange(t0, t0 + file_length, 1 / sample_rate)
        ).all()

    data, times = io.read_timeseries(
        fname, list(datasets.keys()), array_like=True
    )
    assert (times == np.arange(t0, t0 + file_length, 1 / sample_rate)).all()
    for i, (_, value) in enumerate(datasets.items()):
        assert (data[i] == value).all()


def test_validate_ts_dict(sample_rate, t0):
    ts_dict = TimeSeriesDict()
    data = np.arange(0, 1024 * sample_rate)
    ts_dict["test"] = TimeSeries(data, dt=1 / sample_rate, t0=t0)

    io._validate_ts_dict(ts_dict)

    ts_dict["test2"] = TimeSeries(data, dt=1 / sample_rate, t0=t0 + 1)

    with pytest.raises(ValueError):
        io._validate_ts_dict(ts_dict)

    ts_dict["test2"] = TimeSeries(data, dt=1 / (sample_rate + 1), t0=t0)

    with pytest.raises(ValueError):
        io._validate_ts_dict(ts_dict)

    data = np.arange(0, 1025 * sample_rate)
    ts_dict["test2"] = TimeSeries(data, dt=1 / (sample_rate), t0=t0)

    with pytest.raises(ValueError):
        io._validate_ts_dict(ts_dict)

    data = np.arange(0, 1024 * sample_rate)
    ts_dict["test2"] = TimeSeries(data, dt=1 / (sample_rate), t0=t0)


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
        typed_file_names, start=t0, end=t0 + file_length - 1
    )
    assert len(result) == 1
    assert all([isinstance(i, expected_type) for i in result])

    # now test with t0 and tf
    # such that expect two files
    # (only run if number of files greater than 1)
    if n_files > 1:
        result = io.filter_and_sort_files(
            typed_file_names, start=t0, end=t0 + file_length + 1
        )
        assert len(result) == 2
        assert all([isinstance(i, expected_type) for i in result])

    # now test with t0 before start
    # such that all files should be returned
    result = io.filter_and_sort_files(
        typed_file_names,
        start=t0 - 1,
    )
    print(result, expected_names)
    assert len(result) == n_files
    assert all([isinstance(i, expected_type) for i in result])
    assert all([i == j for i, j in zip(result, expected_names)])

    # now test with tf greater than
    # end of files such that all files should be returned
    tf = t0 + 1 + (n_files * file_length)
    result = io.filter_and_sort_files(typed_file_names, end=tf)
    assert len(result) == n_files

    # now test with t0 greater than
    # end of files such that all files should be returned
    tf = t0 + 1 + (n_files * file_length)
    result = io.filter_and_sort_files(typed_file_names, start=tf)
    assert len(result) == 0

    expected_names = [Path(i).name for i in expected_names]
    matches = io.filter_and_sort_files(typed_file_names, return_matches=True)
    assert len(matches) == len(expected_names)
    assert all([i.string == j for i, j in zip(matches, expected_names)])


def test_write_timeseries(
    write_dir, prefix, file_format, t0, sample_rate, file_length, channel_names
):

    datasets = {}
    times = np.arange(t0, t0 + file_length, 1 / sample_rate)

    for channel_name in channel_names:
        datasets[channel_name] = np.arange(
            0,
            sample_rate * file_length,
        )

    fname = io.write_timeseries(
        write_dir, times, prefix, file_format, **datasets
    )

    assert fname.name == f"{prefix}-{int(t0)}-{int(file_length)}.hdf5"

    check_file_contents(fname, sample_rate, t0, file_length, **datasets)


def test_read_timeseries(
    file_names, t0, n_files, file_length, channel_names, sample_rate
):

    write_dir = file_names[0].parent

    # first try reading when passing
    # the write directory
    data, times = io.read_timeseries(
        write_dir, channel_names, t0, t0 + 1000, array_like=True
    )

    assert (times == np.arange(t0, t0 + 1000, 1 / sample_rate)).all()
    assert data.shape == (len(channel_names), sample_rate * 1000)
    for i, dataset in enumerate(data):
        assert (dataset == np.arange(0, 1000 * sample_rate) * (i + 1)).all()

    # now try reading when passing
    # list of files
    data, times = io.read_timeseries(
        file_names, channel_names, t0, t0 + 1000, array_like=True
    )

    for i, dataset in enumerate(data):
        assert (dataset == np.arange(0, 1000 * sample_rate) * (i + 1)).all()

    assert (times == np.arange(t0, t0 + 1000, 1 / sample_rate)).all()
    assert data.shape == (len(channel_names), sample_rate * 1000)

    # now try reading when passing
    # single file
    data, times = io.read_timeseries(
        file_names[0], channel_names, t0, t0 + file_length - 1, array_like=True
    )

    for i, dataset in enumerate(data):
        assert (
            dataset == np.arange(0, (file_length - 1) * sample_rate) * (i + 1)
        ).all()

    assert (
        times == np.arange(t0, t0 + file_length - 1, 1 / sample_rate)
    ).all()
    assert data.shape == (len(channel_names), sample_rate * (file_length - 1))

    # TODO: test when array_like is False


def test_fetch_timeseries():
    CHANNELS = ["H1:STRAIN", "L1:STRAIN"]
    OPEN_CHANNELS = ["V1", "H1"]

    ts = TimeSeries([0, 1], times=[0, 1])
    ts_dict = TimeSeriesDict({channel: ts for channel in CHANNELS})
    mock_get = patch("mldatafind.io.TimeSeriesDict.get", return_value=ts_dict)
    mock_fetch_open = patch(
        "mldatafind.io.TimeSeries.fetch_open_data", return_value=ts
    )

    # test only passing nds2 channels
    with mock_get as mock_get, mock_fetch_open as mock_fetch_open:
        output = io.fetch_timeseries(CHANNELS, 0, 1)
        assert list(output.keys()) == CHANNELS
        get_call = call(CHANNELS, start=0, end=1, verbose=False)
        mock_get.assert_has_calls([get_call])
        mock_fetch_open.assert_not_called()

        # reset mocks
        mock_get.reset_mock()
        mock_fetch_open.reset_mock()

        # test only passing open channels
        output = io.fetch_timeseries(OPEN_CHANNELS, 0, 1)
        assert list(output.keys()) == OPEN_CHANNELS
        mock_get.assert_not_called()
        mock_fetch_open.assert_has_calls(
            [call(channel, 0, 1, verbose=False) for channel in OPEN_CHANNELS]
        )

        # reset mocks
        mock_get.reset_mock()
        mock_fetch_open.reset_mock()

        # test passing both nds2 and open channels
        output = io.fetch_timeseries(CHANNELS + OPEN_CHANNELS, 0, 1)
        assert list(output.keys()) == CHANNELS + OPEN_CHANNELS

        get_call = call(CHANNELS, start=0, end=1, verbose=False)
        mock_get.assert_has_calls([get_call])
        mock_fetch_open.assert_has_calls(
            [call(channel, 0, 1, verbose=False) for channel in OPEN_CHANNELS]
        )

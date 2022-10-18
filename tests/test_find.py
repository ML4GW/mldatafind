from unittest.mock import patch

import pytest

import mldatafind.find as find
import mldatafind.io as io


def test_data_generator(sample_rate):

    segments = [[0, 2048], [4000, 8000]]
    channels = ["H1:Strain", "L1:Strain"]

    with patch("gwpy.timeseries.TimeSeriesDict.get"):
        iterator = find._data_generator(
            segments, channels, io.fetch_timeseries, 1, True
        )

    expected_iterations = len(segments)
    for i in range(expected_iterations):
        next(iterator)

    with pytest.raises(StopIteration):
        next(iterator)

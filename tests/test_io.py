import shutil
from pathlib import Path

import h5py
import numpy as np
import pytest

from mldatafind import io


@pytest.fixture(scope="function")
def write_dir():
    write_dir = Path("tmp")
    write_dir.mkdir(exist_ok=True)
    yield write_dir
    shutil.rmtree(write_dir)


@pytest.fixture
def t0():
    return 1234567890


@pytest.fixture
def duration():
    return 100


@pytest.fixture
def sample_rate():
    return 256.0


@pytest.fixture
def n_channels():
    return 5


@pytest.fixture()
def channels(duration, n_channels, sample_rate):
    data = {}
    for i in range(n_channels):
        data[str(i)] = np.random.normal(size=(duration * int(sample_rate)))
    return data


def test_write_timeseries(write_dir, t0, sample_rate, channels, duration):

    # test basic usecase:
    # one sample rate for all channels
    path = io.write_timeseries(write_dir, t0, sample_rate, "test", **channels)

    assert path.exists()
    assert path == write_dir / f"test-{t0}-{duration}.hdf5"

    with h5py.File(path, "r") as f:
        assert t0 == f.attrs["t0"]
        for key, value in channels.items():
            assert f[key].attrs["sample_rate"] == sample_rate
            assert (f[key][:] == value).all()

    # test that value error thrown
    # if list of sample rates is passed and
    # length of list is not
    # equal to length of channels

    with pytest.raises(ValueError) as err:
        io.write_timeseries(
            write_dir,
            t0,
            [sample_rate] * (len(channels) - 1),
            "test",
            **channels,
        )

        assert err.starts_with("Only")

    # test that value error thrown
    # if duration of each channel
    # is not the same

    with pytest.raises(ValueError) as err:
        # same number of samples
        # but different sample rates
        # will lead to different durations
        sample_rates = (
            [sample_rate] + (len(channels) - 1) * [sample_rate / 2],
        )
        io.write_timeseries(write_dir, t0, "test", sample_rates, **channels)
        assert err.starts_with("Duration")

    # test that different length
    # channels with same duration
    # (e.g. from different sampling rates)
    # succeeds

    duration = 1
    sample_rates = [2048, 1024]
    channels = dict(nn=np.arange(0, 2048, 1), integrated=np.arange(0, 1024, 1))

    path = io.write_timeseries(write_dir, t0, sample_rates, "test", **channels)

    assert path.exists()
    assert path == write_dir / f"test-{t0}-{duration}.hdf5"

    with h5py.File(path, "r") as f:
        assert t0 == f.attrs["t0"]
        for i, (key, value) in enumerate(channels.items()):
            assert f[key].attrs["sample_rate"] == sample_rates[i]
            assert (f[key][:] == value).all()

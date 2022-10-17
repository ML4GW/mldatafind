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
        data[str(i)] = np.arange(0, (duration * int(sample_rate)), 1) * i
    return data


def test_write_timeseries(write_dir, t0, sample_rate, channels, duration):

    path = io.write_timeseries(write_dir, t0, sample_rate, "test", **channels)

    assert path.exists()
    assert path == write_dir / f"test-{t0}-{duration}.hdf5"

    with h5py.File(path, "r") as f:
        assert t0 == f.attrs["t0"]
        assert sample_rate == f.attrs["sample_rate"]
        assert duration == f.attrs["length"]

        for channel, dataset in channels.items():
            data = f[channel][:]
            assert (data == dataset).all()

    # test that value error raised
    # when all channels aren't the same length
    with pytest.raises(ValueError):
        path = io.write_timeseries(
            write_dir,
            t0,
            sample_rate,
            "test",
            **channels,
            bad_channel=np.array([0, 1, 2]),
        )


def test_read_timeseries(duration, sample_rate, channels):
    t0 = 1234567890

    path = f"prefix-{t0}-{duration}.h5"
    with h5py.File(path, "w") as f:
        for channel, dataset in channels.items():
            dataset = f.create_dataset(channel, data=dataset)
        f.attrs["sample_rate"] = sample_rate
        f.attrs["t0"] = t0
        f.attrs["length"] = duration

    channel_names = list(channels.keys())
    data, times = io.read_timeseries(path, *channel_names)

    for i, (channel, dataset) in enumerate(data.items()):
        assert (dataset == channels[channel]).all()

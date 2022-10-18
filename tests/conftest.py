import shutil
from pathlib import Path

import numpy as np
import pytest
from gwpy.timeseries import TimeSeries, TimeSeriesDict


@pytest.fixture(params=["hdf5"])
def file_format(request):
    return request.param


@pytest.fixture(scope="function")
def write_dir():
    tmpdir = Path(__file__).resolve().parent / "data"
    tmpdir.mkdir(parents=True, exist_ok=False)
    yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.fixture(params=["bbhnet"])
def prefix(request):
    return request.param


@pytest.fixture(params=[1024])
def file_length(request):
    return request.param


@pytest.fixture(params=[256])
def sample_rate(request):
    return request.param


@pytest.fixture()
def t0():
    return 1234567000


@pytest.fixture()
def channel_names(request):
    return ["H1:STRAIN", "L1:STRAIN", "H1:AUX", "L1:AUX"]


@pytest.fixture(params=[1, 3])
def n_files(request):
    return request.param


@pytest.fixture()
def file_names(
    write_dir, channel_names, sample_rate, file_length, prefix, t0, n_files
):
    fnames = []
    num_samples = file_length * sample_rate

    for i in range(n_files):
        ts_dict = TimeSeriesDict()
        start = t0 + i * file_length
        data = np.arange(i * num_samples, (i + 1) * num_samples)
        for j, channel_name in enumerate(channel_names):
            ts_dict[channel_name] = TimeSeries(
                data * (j + 1), dt=1 / sample_rate, t0=start
            )

        fname = write_dir / f"{prefix}-{int(start)}-{int(file_length)}.h5"
        ts_dict.write(fname, format="hdf5")
        fnames.append(fname)

    return fnames


@pytest.fixture(params=[str, Path])
def path_type(request):
    return request.param


# params mean:
# 0: return as individual `path_type`
# None: return filenames as-is
# -1: reverse filename list to test segment ordering
@pytest.fixture(params=[0, None, -1])
def typed_file_names(file_names, path_type, request):
    if request.param is None or request.param == -1:
        # map segment filenames to the specified type
        fnames = list(map(path_type, file_names))

        # if -1 reorder things to make sure Segment can
        # handle ordering them itself
        if request.param == -1:
            fnames = fnames[::-1]
        return fnames
    elif request.param == 0:
        # return a standalone PATH_LIKE object
        return path_type(file_names[0])

import shutil
from pathlib import Path

import numpy as np
import pytest

from mldatafind.io import write_timeseries


@pytest.fixture(scope="function")
def tmpdir():
    tmpdir = Path(__file__).resolve().parent / "tmp"
    tmpdir.mkdir(parents=True, exist_ok=False)
    yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.fixture(params=["deepclean", "bbhnet"])
def prefix(request):
    return request.param


@pytest.fixture(params=[100])
def file_length(request):
    return request.param


@pytest.fixture(params=[256, 512])
def sample_rate(request):
    return request.param


@pytest.fixture()
def t0():
    return 1234567000


@pytest.fixture()
def channel_names(request):
    return ["H1:Strain", "L1:Strain", "H1:Aux", "L1:Aux"]


@pytest.fixture()
def create_test_data(
    tmpdir, channel_names, sample_rate, file_length, prefix, t0
):
    def f(sample_rate, file_length, prefix):
        n_files = 10
        samples_per_file = file_length * sample_rate

        for f in range(n_files):
            channels = {}

            for i, channel_name in enumerate(channel_names):
                channels[channel_name] = np.arange(
                    f * samples_per_file, (f + 1) * samples_per_file, 1
                ) * (i + 1)

            # create one file that is discontigous
            start = f * file_length + t0
            if f == (n_files - 1):
                start += 10

            write_timeseries(
                tmpdir,
                start,
                sample_rate,
                prefix,
                **channels,
            )

        return tmpdir

    return f

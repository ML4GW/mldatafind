import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, call

import pytest

from mldatafind import find


@pytest.fixture
def channels():
    return ["thom", "jonny"]


@pytest.fixture
def s_in_gb(channels):
    s_per_gb = int(1024**3 / (4 * len(channels) * 16384))

    def f(gb: float):
        return int(s_per_gb * gb)

    return f


def test_data_generator_without_chunking(s_in_gb, channels):
    find.MEMORY_LIMIT = 0.1

    loaded = Mock()
    loader = Mock(return_value=loaded)

    segments = [
        (0, s_in_gb(0.08)),
        (s_in_gb(0.09), s_in_gb(0.12)),
        (s_in_gb(0.13), s_in_gb(0.15)),
    ]

    with ThreadPoolExecutor(2) as exc:
        gen = find.data_generator(
            exc,
            segments,
            loader,
            channels=channels,
            chunk_size=None,
            current_memory=None,
            retain_order=False,
        )
        it = iter(gen)
        time.sleep(1e-3)
        loader.assert_called_once_with(channels, *segments[0])

        f = next(it)
        assert f is loaded
        time.sleep(1e-3)
        calls = [call(channels, *i) for i in segments[1:]]
        loader.assert_has_calls(calls)

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
    s_per_gb = int(1024**3 / (8 * len(channels) * 16384))

    def f(gb: float):
        return int(s_per_gb * gb)

    return f


@pytest.fixture(params=[ThreadPoolExecutor])
def exc_type(request):
    return request.param


@pytest.fixture(scope="class")
def set_mem():
    find.MEMORY_LIMIT = 1
    yield
    find.MEMORY_LIMIT = 5


@pytest.mark.usefixtures("set_mem")
class TestDataGeneratorRespectsMemory:
    @pytest.fixture
    def segments(self, s_in_gb):
        return [
            (0, s_in_gb(0.1)),
            (s_in_gb(0.1), s_in_gb(0.9)),
            (s_in_gb(1), s_in_gb(1.25)),
            (s_in_gb(1.3), s_in_gb(1.6)),
        ]

    def test_without_chunking(self, segments, channels, exc_type):
        loaded = Mock()
        loader = Mock(return_value=loaded)
        calls = [call(channels, *i) for i in segments]

        with exc_type(2) as exc:
            gen = find.data_generator(
                exc,
                [i for i in segments],
                loader,
                channels=channels,
                chunk_size=None,
                current_memory=None,
                retain_order=True,
            )
            it = iter(gen)
            f = next(it)
            assert f is loaded
            time.sleep(1e-2)
            loader.assert_called_with(*calls[1].args)

            f = next(it)
            time.sleep(1e-1)
            loader.assert_has_calls(calls, any_order=True)

    def test_with_chunking(self, segments, channels, s_in_gb, exc_type):
        find.MEMORY_LIMIT = 0.5

        loaded = Mock()
        loader = Mock(return_value=loaded)

        chunk_size = s_in_gb(0.15)
        calls = []
        for start, stop in segments:
            duration = stop - start
            num_chunks = (duration - 1) // chunk_size + 1
            for i in range(num_chunks):
                begin = start + i * chunk_size
                end = min(start + (i + 1) * chunk_size, stop)
                expected = call(channels, begin, end)
                calls.append(expected)

        with exc_type(2) as exc:
            gen = find.data_generator(
                exc,
                [i for i in segments],
                loader,
                channels=channels,
                chunk_size=chunk_size,
                retain_order=True,
                current_memory=None,
            )
            it = iter(gen)

            # the first segment is shorter than the
            # chunk length, and so it should be a
            # length one generator
            subgen = next(it)
            subit = iter(subgen)
            f = next(subit)
            assert f is loaded
            with pytest.raises(StopIteration):
                next(subit)

            # ensure that at this point our loader has
            # only been called once on the first segment
            loader.assert_called_once_with(*calls[0].args)

            # now load the next generator, which should
            # have six segments, the last of which is
            # shorter than the rest
            subgen = next(it)
            subit = iter(subgen)
            f = next(subit)

            # after the first `next` has been called,
            # the first 4 chunks from this segment should
            # have been submitted: 1 for the one we just
            # got, and then 3 more to fill up the memory
            # after that one was gotten
            time.sleep(1e-3)
            loader.assert_has_calls(calls[1:4], any_order=True)
            with pytest.raises(AssertionError):
                loader.assert_has_calls(calls[4:], any_order=True)

            # now finish iterating through and ensure
            # the generator has the appropriate length
            for i in range(5):
                f = next(subit)
            with pytest.raises(StopIteration):
                next(subit)

            # now make sure all the expected calls
            # to load have now been made
            loader.assert_has_calls(calls[:7], any_order=True)
            with pytest.raises(AssertionError):
                loader.assert_has_calls(calls[7:], any_order=True)

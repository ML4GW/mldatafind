import re
from pathlib import Path
from typing import Iterable, List, Tuple, Union

import h5py
import numpy as np

PATH_LIKE = Union[str, Path]
MAYBE_PATHS = Union[PATH_LIKE, Iterable[PATH_LIKE]]

prefix_re = "[a-zA-Z0-9_:-]+"
t0_re = "[0-9]{10}"
length_re = "[1-9][0-9]{0,3}"
fname_re = re.compile(
    f"(?P<prefix>{prefix_re})-"
    f"(?P<t0>{t0_re})-"
    f"(?P<length>{length_re})"
    ".(?P<suffix>gwf|hdf5|h5)$"
)


def filter_and_sort_files(
    fnames: MAYBE_PATHS, return_matches: bool = False
) -> List[PATH_LIKE]:
    """Sort data files by their timestamps

    Given a list of filenames or a directory name containing
    data files, sort the files by their timestamps, assuming
    they follow the convention <prefix>-<timestamp>-<length>.hdf5
    """

    if isinstance(fnames, (Path, str)):
        # if we passed a single string or path, assume that
        # this refers to a directory containing files that
        # we're meant to sort
        fname_path = Path(fnames)
        if not fname_path.is_dir():
            raise ValueError(f"'{fnames}' is not a directory")

        fnames = list(fname_path.iterdir())
        fname_it = [f.name for f in fnames]
    else:
        # otherwise make sure the iterable contains either
        # _all_ Paths or _all_ strings. If all paths, normalize
        # them to just include the terminal filename
        if all([isinstance(i, Path) for i in fnames]):
            fname_it = [f.name for f in fnames]
        elif not all([isinstance(i, str) for i in fnames]):
            raise ValueError(
                "'fnames' must either be a path to a directory "
                "or an iterable containing either all strings "
                "or all 'pathlib.Path' objects, instead found "
                + ", ".join([type(i) for i in fnames])
            )
        else:
            fname_it = [Path(f).name for f in fnames]

    # use the timestamps from all valid timestamped filenames
    # to sort the files as the first index in a tuple
    matches = zip(map(fname_re.search, fname_it), fnames)
    tups = [(m.group("t0"), f, m) for m, f in matches if m is not None]

    # if return_matches is True, return the match object,
    # otherwise just return the raw filename
    return_idx = 2 if return_matches else 1
    return [t[return_idx] for t in sorted(tups)]


def read_timeseries(path: Path, *channels: str) -> Tuple["np.ndarray", ...]:
    """
    Read multiple channel timeseries from an h5 file

    Args:
        path: path to h5 file to read
        channels: channel name to read

    Returns tuple where the first n_channel elements correspond to
    the datasets of the specified channels, and the last element
    is the array of corresponding times
    """

    with h5py.File(path, "r") as f:
        t0 = f["t0"][:]
        sample_rate = f["sample_rate"][:]

        outputs = []
        for channel in channels:
            try:
                dataset = f[channel]
            except KeyError:
                raise ValueError(
                    "Data file {} doesn't contain channel {}".format(
                        path.fname, channel
                    )
                )
            outputs.append(dataset[:].reshape(-1))

        duration = outputs.shape[-1] / sample_rate
        times = np.arange(0, t0 + duration, 1 / sample_rate)
    return tuple(outputs) + (times,)

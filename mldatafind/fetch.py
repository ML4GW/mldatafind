from typing import List, Literal

import lal
from gwpy.timeseries import TimeSeries, TimeSeriesDict

from mldatafind.authenticate import authenticate

# channel names that signal to fetch open data
OPEN_DATA_CHANNELS = ["H1", "L1", "V1"]


def _fetch_open_data(
    ifos: List[str], start: float, end: float, **kwargs
) -> TimeSeriesDict:
    ts_dict = TimeSeriesDict()
    for ifo in ifos:
        ts_dict[ifo] = TimeSeries.fetch_open_data(ifo, start, end, **kwargs)
    return ts_dict


def fetch(
    start: float,
    end: float,
    channels: list[str],
    sample_rate: float,
    nproc: int = 3,
    verbose: bool = True,
    allow_tape: bool = True,
    resample_method: Literal["gwpy", "lal"] = "gwpy",
) -> TimeSeriesDict:
    """
    Simple wrapper to annotate and simplify
    the kwargs so that jsonargparse can build
    a parser out of them.
    """

    open_data_channels = list(
        filter(lambda x: x in OPEN_DATA_CHANNELS, channels)
    )
    channels = list(filter(lambda x: x not in OPEN_DATA_CHANNELS, channels))

    # fetch data from nds2
    data = TimeSeriesDict()
    if channels:
        # only auth if we have non open channels to fetch
        authenticate()
        for channel in channels:
            ifo = channel.split(":")[0]
            data[ifo] = TimeSeries.get(
                channel,
                start=start,
                end=end,
                verbose=verbose,
                nproc=nproc,
                allow_tape=allow_tape,
            )

    # fetch open data channels and combine
    if open_data_channels:
        open_data_ts_dict = _fetch_open_data(
            open_data_channels,
            start=start,
            end=end,
            verbose=verbose,
            nproc=nproc,
        )
        data.update(open_data_ts_dict)

    if resample_method == "gwpy":
        data = data.resample(sample_rate)
    elif resample_method == "lal":
        lal_data = data.to_lal()
        lal.ResampleREAL8TimeSeries(lal_data, float(1 / sample_rate))
        data = TimeSeries(
            lal_data.data.data,
            epoch=lal_data.epoch,
            dt=lal_data.deltaT,
        )
    else:
        raise ValueError(f"Invalid resampling method {resample_method}")

    return data

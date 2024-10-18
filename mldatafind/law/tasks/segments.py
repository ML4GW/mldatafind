import luigi

from mldatafind.law.base import DataTask
from mldatafind.law.parameters import PathParameter
from mldatafind.law.targets import s3_or_local


class Query(DataTask):
    """
    Law task to query data quality segments
    """

    start = luigi.FloatParameter(description="Start time of segments to query")
    end = luigi.FloatParameter(description="End time of segments to query")
    segments_file = PathParameter(
        description="Output path where segments are written",
    )
    ifos = luigi.ListParameter(
        description="List of ifos to query segments for. "
    )
    flag = luigi.Parameter(
        description="Data quality flag to query. If 'DATA', "
        "will query for open data segments."
    )
    min_duration = luigi.OptionalFloatParameter(
        description="Minimum duration of segments to query. "
        "Any segments of shorter length will be discarded",
        default="",
    )

    retry_count = 5

    def output(self):
        return s3_or_local(self.segments_file, format="txt")

    def get_flags(self):
        if self.flag == "DATA":
            flags = [f"{ifo}_DATA" for ifo in self.ifos]  # open data flags
        else:
            flags = [f"{ifo}:{self.flag}" for ifo in self.ifos]
        return flags

    def run(self):
        from mldatafind.segments import DataQualityDict

        flags = self.get_flags()
        segments = DataQualityDict.query_segments(
            flags,
            self.start,
            self.end,
            self.min_duration,
        )
        with self.output().open("w") as f:
            segments.write(f, format="segwizard")

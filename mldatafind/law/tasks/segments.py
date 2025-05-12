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
        description="List of ifos for which "
        "to query segments, and strain data. "
    )
    flags = luigi.ListParameter(
        description="Data quality flag to query for each ifo. "
        "If '{ifo}_DATA', will query for open data segments. "
        "Expect one value per ifo in `ifos`. "
    )
    min_duration = luigi.OptionalFloatParameter(
        description="Minimum duration of segments to query. "
        "Any segments of shorter length will be discarded",
        default="",
    )

    retry_count = 5

    def output(self):
        return s3_or_local(self.segments_file, format="txt")

    def run(self):
        from mldatafind.segments import DataQualityDict

        segments = DataQualityDict.query_segments(
            self.flags,
            self.start,
            self.end,
            self.min_duration,
        )
        with self.output().open("w") as f:
            segments.write(f, format="segwizard")

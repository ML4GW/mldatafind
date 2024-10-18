import law
import luigi
from luigi.util import inherits

from mldatafind.law.base import DataTask
from mldatafind.law.parameters import OptionalPathParameter, PathParameter
from mldatafind.law.targets import s3_or_local
from mldatafind.law.tasks.condor.workflows import StaticMemoryWorkflow
from mldatafind.law.tasks.segments import Query


@inherits(Query)
class Fetch(law.LocalWorkflow, StaticMemoryWorkflow, DataTask):
    """
    Law workflow for fetching strain data
    """

    data_dir = PathParameter(
        description="Directory to store fetched data. "
        "Can be a local path or an s3 path."
    )
    segments_file = OptionalPathParameter(
        description="Path where segments file will be stored. "
        "If not provided, will use the "
        "segments file from the Query task.",
        default="",
    )
    sample_rate = luigi.FloatParameter(
        description="Rate at which fetched data will be sampled in Hz"
    )
    channels = luigi.ListParameter(
        description="List of channels to fetch",
    )
    max_duration = luigi.OptionalFloatParameter(
        description="Maximum duration for each segment in seconds. "
        "Segments of longer length will be split into multiple segments. "
        "Default is no maximum duration.",
        default="",
    )
    prefix = luigi.Parameter(default="background")

    exclude_params_req = {"condor_directory"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not str(self.data_dir).startswith("s3://"):
            self.data_dir.mkdir(exist_ok=True, parents=True)

        if not self.segments_file:
            self.segments_file = self.data_dir / "segments.txt"

    @law.dynamic_workflow_condition
    def workflow_condition(self) -> bool:
        return self.workflow_input()["segments"].exists()

    def load_segments(self):
        with self.workflow_input()["segments"].open("r") as f:
            segments = f.read().splitlines()[1:]
        return segments

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        segments = self.load_segments()
        branch_map, i = {}, 1
        for segment in segments:
            segment = segment.split("\t")
            start, duration = map(float, segment[1::2])
            step = duration if self.max_duration is None else self.max_duration
            num_steps = (duration - 1) // step + 1

            for j in range(int(num_steps)):
                segstart = start + j * step
                segdur = min(start + duration - segstart, step)
                branch_map[i] = (segstart, segdur)
                i += 1
        return branch_map

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["segments"] = Query.req(self, segments_file=self.segments_file)
        return reqs

    @workflow_condition.output
    def output(self):
        start, duration = self.branch_data
        start = int(float(start))
        duration = int(float(duration))
        fname = "{}-{}-{}.hdf5".format(self.prefix, start, duration)
        fname = self.data_dir / fname
        return s3_or_local(fname)

    def run(self):
        import h5py

        from mldatafind.fetch import fetch

        start, duration = self.branch_data
        start = int(float(start))
        duration = int(float(duration))

        X = fetch(
            start,
            start + duration,
            self.channels,
            self.sample_rate,
        )
        size = int(duration * self.sample_rate)
        with self.output().open("w") as f:
            with h5py.File(f, "w") as h5file:
                # write with chunking for dataloading perf increase
                X.write(
                    h5file,
                    format="hdf5",
                    chunks=(min(size, 131072),),
                    compression=None,
                )

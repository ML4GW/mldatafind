from law.contrib.slurm import SlurmWorkflow
from mldatafind.law.parameters import PathParameter
import luigi
import law
import os
from mldatafind.law.base import DATAFIND_ENV_VARS

class DeltaSlurmWorkflow(SlurmWorkflow):
    """
    Slurm workflow aimed to be compatible with the Delta cluster.
    """

    slurm_partition = luigi.Parameter(
        default="",
        significant=True,
        description="target queue partition; default: empty",
    )

    slurm_account = luigi.Parameter(
        significant=False,
        description="target account"
    )
    slurm_directory = PathParameter(
        description="Directory where store slurm job files will be written"
    )

    mem = luigi.Parameter(
        default="4G",
        description="Memory requested for each job",
    )


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        law.config.update(
            {
                "job": {
                    "job_file_dir_cleanup": "False",
                    "job_file_dir_mkdtemp": "False",
                }
            }
        )

    @property
    def name(self):
        return self.__class__.__name__.lower()

    @property
    def job_file_dir(self):
        return self.slurm_output_directory().child("jobs", type="d").path
    
    @property
    def law_config(self):
        path = os.getenv("LAW_CONFIG_FILE", "")
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)
        return path
    
    def slurm_log_directory(self):
        target = law.LocalDirectoryTarget(self.slurm_directory)
        if not target.exists():
            target.makedirs()
        return law.LocalDirectoryTarget(self.slurm_directory / "logs") 

    # TODO: remove this when slurm_log_directory is fixed
    def htcondor_log_directory(self):
        return self.slurm_log_directory() 

    def slurm_create_job_file_factory(self, **kwargs):
        # set the job file dir to proper location
        kwargs["dir"] = self.job_file_dir
        return super().slurm_create_job_file_factory(**kwargs)

    def slurm_output_directory(self):
        target = law.LocalDirectoryTarget(self.slurm_directory)
        if not target.exists():
            target.makedirs()
        return law.LocalDirectoryTarget(self.slurm_directory)

    def slurm_use_local_scheduler(self):
        return True


    def build_environment(self):
        # set necessary env variables for
        # required for remote data access
        environment = ""
        for envvar in DATAFIND_ENV_VARS:
            value = os.getenv(envvar)
            if value is not None:
                environment += f"{envvar}={value},"

        # forward current path and law config
        environment += f"PATH={os.getenv('PATH')},"
        environment += f"LAW_CONFIG_FILE={self.law_config},"
        environment += f"USER={os.getenv('USER')}"
        return environment

    def append_logs(self, config):
        for output in ["output", "error"]:
            ext = output[:3]
            config.custom_content.append(
                (
                    output,
                    os.path.join(
                        self.slurm_log_directory().path,
                        f"{self.name}-%j.{ext}",
                    ),
                )
            )
    
    def slurm_job_config(self, config, job_num, branches):
        config.custom_content.append(("nodes", 1))
        config.custom_content.append(("account", self.slurm_account))
        config.custom_content.append(("mem", self.mem))
        config.custom_content.append(("export", self.build_environment()))
        self.append_logs(config)
        return config
    

import os

import law
import luigi
from law.contrib import htcondor

from mldatafind.law.base import DATAFIND_ENV_VARS
from mldatafind.law.parameters import PathParameter


class LDGCondorWorkflow(htcondor.HTCondorWorkflow):
    """
    Base class for law workflows that run via condor on LDG
    """

    condor_directory = PathParameter(
        description="Directory where store condor log files will be written"
    )
    accounting_group_user = luigi.Parameter(
        description="Condors accounting group user name. Defaults to the "
        "`LIGO_USERNAME` environment variable",
        default=os.getenv("LIGO_USERNAME"),
    )
    accounting_group = luigi.Parameter(
        description="Condor accounting group name. Defaults to the "
        "`LIGO_GROUP` environment variable",
        default=os.getenv("LIGO_GROUP"),
    )
    request_disk = luigi.Parameter(
        description="Disk space requested for each job", default="1024"
    )
    request_memory = luigi.Parameter(
        description="Memory requested for each job", default="32678"
    )
    request_cpus = luigi.IntParameter(
        description="Number of cpus requested for each job", default=1
    )

    # don't want to share these between condor tasks via .req()
    exclude_params_req = {"request_memory", "request_disk", "request_cpus"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.htcondor_log_dir.touch()
        self.htcondor_output_directory().touch()

        law.config.update(
            {
                "job": {
                    "job_file_dir": self.job_file_dir,
                    "job_file_dir_cleanup": "False",
                    "job_file_dir_mkdtemp": "False",
                }
            }
        )

    @property
    def name(self):
        return self.__class__.__name__.lower()

    @property
    def htcondor_log_dir(self):
        return law.LocalDirectoryTarget(self.condor_directory / "logs")

    @property
    def job_file_dir(self):
        return self.htcondor_output_directory().child("jobs", type="d").path

    @property
    def law_config(self):
        path = os.getenv("LAW_CONFIG_FILE", "")
        if not os.path.isabs(path):
            path = os.path.join(os.getcwd(), path)
        return path

    def build_environment(self):
        # set necessary env variables for
        # required for LDG data access
        environment = '"'
        for envvar in DATAFIND_ENV_VARS:
            value = os.getenv(envvar)
            if value is not None:
                environment += f"{envvar}={value} "

        # forward current path and law config
        environment += f'PATH={os.getenv("PATH")} '
        environment += f"LAW_CONFIG_FILE={self.law_config} "
        environment += f"USER={os.getenv('USER')} "
        environment += f"TMPDIR={os.getenv('TMPDIR')} "
        return environment

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(self.condor_directory)

    def htcondor_use_local_scheduler(self):
        return True

    def append_memory(self):
        raise NotImplementedError

    def append_logs(self, config):
        for output in ["log", "output", "error"]:
            ext = output[:3]
            config.custom_content.append(
                (
                    output,
                    os.path.join(
                        self.htcondor_log_dir.path,
                        f"{self.name}-$(Cluster).{ext}",
                    ),
                )
            )

    def htcondor_job_config(self, config, job_num, branches):
        environment = self.build_environment()
        config.custom_content.append(("environment", environment))
        config.custom_content.append(("stream_error", "True"))
        config.custom_content.append(("stream_output", "True"))
        config.custom_content.append(
            ("accounting_group", self.accounting_group)
        )
        config.custom_content.append(
            ("accounting_group_user", self.accounting_group_user)
        )
        config.custom_content.append(("request_disk", self.request_disk))
        config.custom_content.append(("request_cpus", self.request_cpus))
        self.append_memory(config)
        self.append_logs(config)
        return config

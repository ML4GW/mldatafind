import os
from pathlib import Path

import law
import luigi
from law.contrib import singularity
from law.contrib.singularity.config import config_defaults

root = Path(__file__).resolve().parent.parent.parent

DATAFIND_ENV_VARS = [
    "KRB5_KTNAME",
    "X509_USER_PROXY",
    "GWDATAFIND_SERVER",
    "NDSSERVER",
    "LIGO_USERNAME",
    "DEFAULT_SEGMENT_SERVER",
    "AWS_ENDPOINT_URL",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ACCESS_KEY_ID",
]


class DataSandbox(singularity.SingularitySandbox):
    """
    Singularity sandbox for running mldatafind tasks
    """

    sandbox_type = "mldatafind"

    def get_custom_config_section_postfix(self):
        return self.sandbox_type

    @classmethod
    def config(cls):
        config = {}
        default = config_defaults(None).pop("singularity_sandbox")
        default["law_executable"] = "/opt/env/bin/law"
        default["forward_law"] = False
        postfix = cls.sandbox_type
        config[f"singularity_sandbox_{postfix}"] = default
        return config

    @property
    def data_directories(self):
        """
        Data directories on LDG clusters to bind to the container
        to enable local data discovery
        """
        return map(
            Path, ["/cvmfs", "/hdfs", "/gpfs", "/ceph", "/hadoop", "/archive"]
        )

    def _get_volumes(self):
        volumes = super()._get_volumes()
        # bind data directories if they
        # exist on the local cluster
        for dir in self.data_directories:
            if dir.exists():
                volumes[str(dir)] = str(dir)

        # bind users /local directory for
        # storing large tmp files,
        # e.g. for local storage before
        # being dumped to s3 by luigi
        tmpdir = Path(f"/local/{os.getenv('USER')}")
        if tmpdir.exists():
            tmpdir = str(tmpdir)
            volumes[tmpdir] = tmpdir

        # bind aws directory that contains s3 credentials
        aws_dir = os.path.expanduser("~/.aws/")
        volumes[aws_dir] = aws_dir
        return volumes

    def _get_env(self):
        env = super()._get_env()
        for envvar in DATAFIND_ENV_VARS:
            value = os.getenv(envvar)
            if value is not None:
                env[envvar] = value
        return env


law.config.update(DataSandbox.config())


class DataTask(law.SandboxTask):
    """
    law SandboxTask for running mldatafind workflows
    """

    image = luigi.PathParameter(
        default=os.getenv("MLDATAFIND_CONTAINER", ""),
        significant=False,
        description="Path to the singularity container to use for the task. "
        "Defaults to the `MLDATAFIND_CONTAINER` environment variable. ",
    )

import os

import luigi
from luigi.contrib.s3 import S3Client


class s3(luigi.Config):
    """
    Global S3 client configuration.
    """

    endpoint_url = luigi.Parameter(
        description="S3 endpoint URL. Defaults to reading from "
        "`AWS_ENDPOINT_URL` environment variable.",
        default=os.getenv("AWS_ENDPOINT_URL"),
    )
    aws_access_key_id = luigi.Parameter(
        description="AWS access key ID. Defaults to reading from "
        "`AWS_ACCESS_KEY_ID` environment variable.",
        default=os.getenv("AWS_ACCESS_KEY_ID"),
    )
    aws_secret_access_key = luigi.Parameter(
        description="AWS secret access key. Defaults to reading from "
        "`AWS_SECRET_ACCESS_KEY` environment variable.",
        default=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    @property
    def client(self):
        return S3Client(endpoint_url=self.endpoint_url)

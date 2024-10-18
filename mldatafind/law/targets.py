from typing import Literal

import luigi
from luigi.contrib.s3 import S3Target
from luigi.format import BaseWrapper, WrappedFormat

from mldatafind.law.config import s3
from mldatafind.law.parameters import PATH_LIKE


# format for writing h5 files
# to s3 via bytes streams
class BytesFormat(WrappedFormat):
    input = "bytes"
    output = "bytes"
    wrapper_cls = BaseWrapper


Bytes = BytesFormat()


# law targets compatible with luigi S3 targets
class LawS3Target(S3Target):
    optional = False

    def complete(self):
        return self.exists()


class LawLocalTarget(luigi.LocalTarget):
    optional = False

    def complete(self):
        return self.exists()


def s3_or_local(path: PATH_LIKE, format: Literal["hdf5", "txt"] = "hdf5"):
    """
    Dynamically return a LawS3Target or LawLocalTarget depending on
    the specified path.

    Args:
        path:
            The path to the target. If the path starts with "s3://",
            a LawS3Target is returned, otherwise a LawLocalTarget is returned.
        format:
            The format of the target. Can be either 'hdf5' or 'txt'.
            Defaults to "hdf5".

    Returns:
        A LawS3Target or LawLocalTarget
    """
    format = Bytes if format == "hdf5" else None
    path = str(path)
    if path.startswith("s3://"):
        return LawS3Target(
            path,
            client=s3().client,
            ContentType="application/octet-stream",
            format=format,
        )
    else:
        return LawLocalTarget(path, format=format)

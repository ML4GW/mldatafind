from pathlib import Path
from typing import Union

import luigi
from cloudpathlib import CloudPath

PATH_LIKE = Union[CloudPath, Path, str]


class PathParameter(luigi.Parameter):
    """
    luigi `Parameter` class that handles parsing strings
    into pathlib.Path or cloudpathlib.S3Path objects.
    """

    def parse(self, x: PATH_LIKE):
        if isinstance(x, (Path, CloudPath)):
            return x / ""
        if isinstance(x, str):
            if x.startswith("s3://"):
                return CloudPath(x) / ""
            else:
                return Path(x) / ""
        else:
            raise ValueError(
                f"Expected string, Path, or CloudPath, got {type(x)}"
            )

    def serialize(self, x):
        return str(x)

    def normalize(self, x):
        return self.parse(x)

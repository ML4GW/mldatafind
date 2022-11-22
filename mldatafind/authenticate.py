import os
from pathlib import Path

from ciecplib.ui import get_cert
from ciecplib.x509 import check_cert, load_cert, write_cert


def authenticate():
    """
    Authenticate a user to access LIGO data sources

    This function will load a X509 certificate from the environment
    variable `X509_USER_PROXY`. If the credential doesn't exist,
    it will create a new one. If the credential exists and is valid,
    it will continue to use it. Otherwise, it will generate a new credential.

    This function assumes the user has already made kerberos credentials.

    To generate kerberos credentials, run

    ```kinit albert.einstein@LIGO.ORG```

    """

    path = os.getenv("X509_USER_PROXY")

    if path is None:
        raise ValueError("Set X509_USER_PROXY environment variable")

    elif Path(path).exists():
        cert = load_cert(path)
        try:
            check_cert(cert)
        except RuntimeError:
            cert, key = get_cert(kerberos=True)
            write_cert(path, cert, key)
    else:
        cert, key = get_cert(kerberos=True)
        write_cert(path, cert, key)

    return path

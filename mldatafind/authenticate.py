import os

from ciecplib.ui import get_cert
from ciecplib.x509 import check_cert, load_cert, write_cert


def authenticate():
    """
    Authenticate a user to access LIGO data sources

    This function will load a X509 certificate from the environment
    variable X509_USER_PROXY. If not valid, it will generate a new credential.

    """

    path = os.getenv("X509_USER_PROXY")

    if path is None:
        raise ValueError("Set X509_USER_PROXY environment variable")
    else:
        # load the certificate
        cert = load_cert(path)

        try:
            raise RuntimeError
            # validate the certificate
            check_cert(cert)
        except RuntimeError:
            cert, key = get_cert(kerberos=True)

            write_cert(path, cert, key)

    return path

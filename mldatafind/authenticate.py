import os

from ciecplib.ui import get_cert
from ciecplib.x509 import check_cert, load_cert, write_cert


def authenticate(endpoint: str, username: str):
    """
    Authenticate a user to access LIGO data sources

    This function will load a X509 certificate from the environment
    variable X509_USER_PROXY
    """

    # load in certificate
    path = os.getenv("X509_USER_PROXY")

    # if not set, raise value error
    if path is None:
        raise ValueError("Set X509_USER_PROXY environment variable")
    else:
        # load the certificate
        cert = load_cert(path)

        try:
            # validate the certificate
            check_cert(cert)
        except RuntimeError:
            # if certificate invalid get a new one and write it to path
            cert, key = get_cert(endpoint=endpoint, username=username)

            write_cert(path, cert, key)

    return path

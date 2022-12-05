import os
import shutil
import subprocess
from pathlib import Path

from ciecplib.ui import get_cert
from ciecplib.x509 import check_cert, load_cert, write_cert


def _validate_env(env_var: str):
    """
    Validate the existence of an environment variable
    Args:
        env_var : str
            The name of the environment variable
    Raises:
        ValueError If the environment variable is not set

    Returns The value of the environment variable
    """
    value = os.getenv(env_var)
    if value is None:
        raise ValueError(f"{env_var} environment variable not set")

    return value


def authenticate():
    """
    Authenticate a user to access LIGO data sources

    This function will load a X509 certificate from the environment
    variable `X509_USER_PROXY`. If the credential doesn't exist,
    it will create a new one. If the credential exists and is valid,
    it will continue to use it. Otherwise, it will generate a new credential.

    If generating new credential, a kerberos keytab is required
    for passwordless authentication. It's location should be
    specified in the environment variable `KRB5_KTNAME`.
    This function assumes the user has already generated a kerberos keytab
    with principal user.name@LIGO.ORG. This function will read in username
    from the environment variable `LIGO_USERNAME`

    For instructions on generating a kerberos keytab,
    see https://computing.docs.ligo.org/guide/auth/kerberos/

    """

    user = _validate_env("LIGO_USERNAME")
    keytab_location = _validate_env("KRB5_KTNAME")
    path = _validate_env("X509_USER_PROXY")

    kinit_command = shutil.which("kinit")
    if kinit_command is None:
        raise ValueError("kinit command not found")

    args = [
        kinit_command,
        "-p",
        f"{user}@LIGO.ORG",
        "-k",
        "-t",
        keytab_location,
    ]
    subprocess.run(args, check=True)

    path = os.getenv("X509_USER_PROXY")

    if path is None:
        raise ValueError("X509_USER_PROXY environment variable not set")

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

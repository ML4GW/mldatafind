import logging
from mldatafind.authenticate.x509 import authenticate as x509_auth
from mldatafind.authenticate.scitoken import authenticate as scitoken_auth


def authenticate():
    """
    Try authenticating to x509 or scitokens,
    handling any exceptions
    """
    # only auth if we have non open channels to fetch
    # first try x509 (for querying from nds2)
    # then create scitokens
    try:
        x509_auth()
    except ValueError as e:
        logging.warning(f"Failed to authenticate to x509: {e}")

    # if running locally, can try to gen scitoken
    # if running via condor, this will fail,
    # but condor token generating mechanism will work.
    try:
        scitoken_auth()
    except SystemExit as e:
        logging.warning(str(e))

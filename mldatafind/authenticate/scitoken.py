from htgettoken import main


def authenticate():
    # generates with gracedb, frames, gwdatafind and dqsegdb scopes.
    # See https://computing.docs.ligo.org/guide/auth/scitokens/#get
    args = [
        "-a",
        "vault.ligo.org",
        "-i",
        "igwn",
    ]
    main(args)

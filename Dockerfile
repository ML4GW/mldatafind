FROM continuumio/miniconda3:4.12.0

# set up some conda stuff
SHELL ["/bin/bash", "-c"]
ENV CONDA_INIT=$CONDA_PREFIX/etc/profile.d/conda.sh \
    DEBIAN_FRONTEND=noninteractive

# install kerberos executables
RUN apt-get update \
        \
        && printf '[libdefaults]\n  default_realm = LIGO.ORG\n' > /etc/krb5.confg \
        \
        && apt-get install -y --no-install-recommends \
            krb5-user \
            build-essential \
        \
        && rm -rf /var/lib/apt/lists/*

# install mldatafind
COPY . /opt/mldatafind
RUN python -m pip install poetry==1.2.0b3 \
        \
        && conda env create -f /opt/mldatafind/environment.yaml \
        \
        && source $CONDA_INIT \
        \
        && conda activate mldatafind \
        \
        && python -m pip install /opt/mldatafind \
        \
        && sed -i 's/conda activate base/conda activate mldatafind/g' ~/.bashrc

# required environment variables with their default locations
ENV LIGO_USERNAME=albert.einstein \
    KRB5_KTNAME=/root/.kerberos/ligo.org.keytab \
    X509_USER_PROXY=/root/cilogon_cert/CERT_KEY.pem
VOLUME /root/.kerberos /root/cilogon_cert

FROM continuumio/miniconda3:4.12.0

# set up some conda stuff
SHELL ["/bin/bash", "-c"]
ENV CONDA_INIT=$CONDA_PREFIX/etc/profile.d/conda.sh

# install kinit utility
RUN apt-get update \
        \
        && apt-get install -y --no-install-recommends \
            wget \
            build-essential \
            bison \
        \
        && wget http://web.mit.edu/kerberos/dist/krb5/1.20/krb5-1.20.1.tar.gz \
        \
        && tar -xzf krb5-1.20.1.tar.gz \
        \
        && cd krb5-1.20.1/src \
        \
        && ./configure \
        \
        && make \
        \
        && make install \
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

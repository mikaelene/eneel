FROM python:3.8-slim

# Installing Oracle instant client and support for odbc
WORKDIR    /opt/oracle
RUN        apt-get update \
           && apt-get install -y gcc g++ unixodbc unixodbc-dev libaio1 wget unzip git \
           && wget https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-basic-linux.x64-21.5.0.0.0dbru.zip \
           && wget https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-sqlplus-linux.x64-21.5.0.0.0dbru.zip \
           && unzip instantclient-basic-linux.x64-21.5.0.0.0dbru.zip \
           && unzip instantclient-sqlplus-linux.x64-21.5.0.0.0dbru.zip \
           && rm -f instantclient-basic-linux.x64-21.5.0.0.0dbru.zip \
           && rm -f instantclient-sqlplus-linux.x64-21.5.0.0.0dbru.zip \
           && cd /opt/oracle/instantclient* \
           && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
           && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
           && ldconfig

ENV PATH /opt/oracle/instantclient_21_5:$PATH


# install dependencies
WORKDIR    /
COPY eneel /eneel
COPY setup.py /
RUN python setup.py install

# If you want to install the latest version from git
# RUN pip install git+https://github.com/mikaelene/eneel.git
FROM python:3.8-slim

# Installing Oracle instant client
WORKDIR    /opt/oracle
RUN        apt-get update \
           && apt-get install -y gcc g++ unixodbc unixodbc-dev libaio1 wget unzip git \
            && wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
            && unzip instantclient-basiclite-linuxx64.zip \
            && rm -f instantclient-basiclite-linuxx64.zip \
            && cd /opt/oracle/instantclient* \
            && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
            && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
            && ldconfig

WORKDIR    /

COPY eneel /eneel
COPY example_connections.yml /
COPY example_project.yml /
COPY setup.py /

# install dependencies
# RUN pip install git+https://github.com/mikaelene/eneel.git
RUN python setup.py install

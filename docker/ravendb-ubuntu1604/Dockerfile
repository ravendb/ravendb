FROM ubuntu:16.04

ENV DataDir=APPDRIVE:/databases PublicServerUrl='' PublicTcpServerUrl='' CertificatePath='' UnsecuredAccessAllowed='' LogsMode=''

COPY RavenDB-4.0.*-*-*-ubuntu.16.04-x64.tar.bz2 /opt/RavenDB.tar.bz2
COPY run-raven.sh healthcheck.sh /opt/

HEALTHCHECK --start-period=60s CMD /opt/healthcheck.sh

RUN apt-get update \
    && apt-get install -y \
    && apt-get install --no-install-recommends bzip2 libunwind8 libicu55 libcurl3 ca-certificates -y \
    && cd /opt \
    && mkdir RavenDB \
    && tar xjvf RavenDB.tar.bz2 \
    && rm RavenDB.tar.bz2 \
    && apt-get remove bzip2 -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/RavenDB/Server
VOLUME /databases /opt/raven-settings.json
EXPOSE 8080 38888
CMD /opt/run-raven.sh

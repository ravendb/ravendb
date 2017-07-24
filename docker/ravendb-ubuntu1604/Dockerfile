FROM ubuntu:16.04

ENV SecurityAuthenticationEnabled=true DataDir=APPDRIVE:/databases PublicServerUrl='' PublicTcpServerUrl=''

COPY RavenDB-4.0.*-*-*-ubuntu.16.04-x64.tar.bz2 /opt/RavenDB.tar.bz2
COPY run-raven.sh /opt/run-raven.sh

# Prepare container
RUN apt-get update \
    && apt-get install -y \
    && apt-get install --no-install-recommends bzip2 libunwind8 libicu55 libcurl3 ca-certificates -y \
    && cd /opt \
    && mkdir RavenDB \
    && tar xjvf RavenDB.tar.bz2 -C RavenDB \
    && rm RavenDB.tar.bz2 \
    && apt-get remove bzip2 -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

VOLUME /databases /opt/raven-settings.json
EXPOSE 8080 38888

CMD /opt/run-raven.sh

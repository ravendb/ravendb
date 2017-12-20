FROM ubuntu:16.04


RUN apt-get update \
    && apt-get install -y \
    && apt-get install --no-install-recommends curl bzip2 libunwind8 libicu55 libcurl3 ca-certificates jq -y

ENV RAVEN_DataDir=/databases RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

EXPOSE 8080 38888 161

COPY RavenDB.tar.bz2 /opt/RavenDB.tar.bz2

RUN cd /opt \
    && tar xjvf RavenDB.tar.bz2 \
    && rm RavenDB.tar.bz2 \
    && apt-get remove bzip2 -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY run-raven.sh healthcheck.sh /opt/RavenDB/
COPY settings.json /opt/RavenDB/Server

HEALTHCHECK --start-period=60s CMD /opt/RavenDB/healthcheck.sh

WORKDIR /opt/RavenDB/Server

VOLUME /databases /opt/RavenDB/cert /opt/RavenDB/secrets

CMD /opt/RavenDB/run-raven.sh

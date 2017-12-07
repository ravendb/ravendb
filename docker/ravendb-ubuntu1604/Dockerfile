FROM ubuntu:16.04


RUN apt-get update \
    && apt-get install -y \
    && apt-get install --no-install-recommends curl bzip2 libunwind8 libicu55 libcurl3 ca-certificates jq -y

ENV DATA_DIR=/databases CUSTOM_CONFIG_FILE='' PUBLIC_SERVER_URL='' PUBLIC_TCP_SERVER_URL='' CERTIFICATE_PATH='' CERTIFICATE_PASSWORD='' CERTIFICATE_PASSWORD_FILE='' UNSECURED_ACCESS_ALLOWED='' LOGS_MODE='' RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

EXPOSE 8080 38888 161

COPY RavenDB.tar.bz2 /opt/RavenDB.tar.bz2

RUN cd /opt \
    && tar xjvf RavenDB.tar.bz2 \
    && rm RavenDB.tar.bz2 \
    && apt-get remove bzip2 -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY run-raven.sh healthcheck.sh /opt/RavenDB/

HEALTHCHECK --start-period=60s CMD /opt/RavenDB/healthcheck.sh

WORKDIR /opt/RavenDB/Server

VOLUME /databases /opt/RavenDB/cert /opt/RavenDB/secrets

CMD /opt/RavenDB/run-raven.sh

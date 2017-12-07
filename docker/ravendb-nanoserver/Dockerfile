FROM microsoft/dotnet:runtime-nanoserver

HEALTHCHECK --start-period=60s CMD powershell -c 'c:\healthcheck.ps1'

ENV DATA_DIR=c:/databases CUSTOM_CONFIG_FILE='' PUBLIC_SERVER_URL='' PUBLIC_TCP_SERVER_URL='' UNSECURED_ACCESS_ALLOWED='' LOGS_MODE='' CERTIFICATE_PATH='' CERTIFICATE_PASSWORD='' CERTIFICATE_PASSWORD_FILE='' RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

VOLUME c:/databases c:/ravendb/cert c:/ravendb/secrets c:/ravendb/config

EXPOSE 8080 38888 161

COPY RavenDB.zip install-raven.ps1 run-raven.ps1 healthcheck.ps1 c:/

RUN powershell -c 'c:\install-raven.ps1'

WORKDIR C:/ravendb/Server

ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll c:/ravendb/server

CMD powershell -c 'c:\run-raven.ps1'

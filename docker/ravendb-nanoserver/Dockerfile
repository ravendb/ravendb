FROM microsoft/dotnet:runtime-nanoserver

HEALTHCHECK --start-period=60s CMD powershell -c 'c:\healthcheck.ps1'

ENV RAVEN_ARGS='' RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

EXPOSE 8080 38888 161

COPY RavenDB.zip install-raven.ps1 run-raven.ps1 healthcheck.ps1 c:/

RUN powershell -c 'c:\install-raven.ps1'

COPY settings.json c:/ravendb/Server

VOLUME c:/ravendb/Server/RavenData c:/ravendb/cert c:/ravendb/secrets c:/ravendb/config

WORKDIR C:/ravendb/Server

ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll c:/ravendb/server

CMD powershell -c 'c:\run-raven.ps1'

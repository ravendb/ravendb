FROM microsoft/dotnet:runtime-nanoserver

HEALTHCHECK --start-period=60s CMD c:\healthcheck.ps1

ENV DataDir=APPDRIVE:/databases CustomConfigFilename='' PublicServerUrl='' PublicTcpServerUrl='' UnsecuredAccessAllowed='' LogsMode=''

VOLUME c:/databases c:/raven-config
EXPOSE 8080 38888

COPY RavenDB.zip install-raven.ps1 run-raven.ps1 healthcheck.ps1 c:/

RUN powershell -c 'c:\install-raven.ps1'
WORKDIR C:/ravendb/Server

ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll c:/ravendb/server
CMD powershell -c 'c:\run-raven.ps1'

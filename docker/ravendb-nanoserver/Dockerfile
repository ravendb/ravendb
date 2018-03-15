FROM microsoft/dotnet:runtime-nanoserver

HEALTHCHECK --start-period=60s CMD powershell -c 'C:\healthcheck.ps1'

ENV RAVEN_ARGS='' RAVEN_SETTINGS='' RAVEN_Setup_Mode='Initial' RAVEN_DataDir='RavenData' RAVEN_ServerUrl_Tcp='38888' RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

EXPOSE 8080 38888 161

COPY RavenDB.zip install-raven.ps1 run-raven.ps1 healthcheck.ps1 c:/

RUN powershell -c 'C:\install-raven.ps1'

COPY settings.json C:/RavenDB/Server

VOLUME C:/RavenDB/Server/RavenData C:/RavenDB/Config

WORKDIR C:/ravendb/Server

ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll C:/RavenDB/Server

CMD [ "powershell", "-File", "C:\\run-raven.ps1" ]

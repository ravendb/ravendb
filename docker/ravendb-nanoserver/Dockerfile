FROM microsoft/dotnet:runtime-nanoserver

ENV AllowEverybodyToAccessTheServerAsAdmin=false DataDir=APPDRIVE:/databases CustomConfigFilename=""

VOLUME c:/databases c:/raven-config
EXPOSE 8080 38888

COPY RavenDB-4.0.*-*-*-windows-x64.zip c:/RavenDB.zip
RUN powershell -c 'mkdir c:/ravendb; expand-archive c:/ravendb.zip -DestinationPath c:/ravendb'
ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll c:/ravendb/server
COPY run-raven.ps1 c:/run-raven.ps1

CMD powershell -c 'c:\run-raven.ps1'

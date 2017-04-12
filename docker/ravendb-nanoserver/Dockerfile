FROM microsoft/dotnet:runtime-nanoserver

ENV AllowEverybodyToAccessTheServerAsAdmin=false DataDir=APPDRIVE:/databases

VOLUME c:/databases

EXPOSE 8080

COPY RavenDB-4.0.*-*-*-windows-x64.zip c:/RavenDB.zip

RUN powershell -c 'mkdir c:/ravendb; expand-archive c:/ravendb.zip -DestinationPath c:/ravendb'

ADD https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll c:/ravendb/server

WORKDIR c:/ravendb/Server

CMD ./Raven.Server.exe /Raven/RunAsService=true /Raven/AllowEverybodyToAccessTheServerAsAdmin=$($env:AllowEverybodyToAccessTheServerAsAdmin) /Raven/DataDir=$($env:DataDir)

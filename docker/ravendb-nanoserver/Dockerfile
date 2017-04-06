FROM microsoft/dotnet:runtime-nanoserver

ENV AllowEverybodyToAccessTheServerAsAdmin true
ENV DataDir APPDRIVE:/databases
VOLUME c:/databases
EXPOSE 8080

ADD http://hibernatingrhinos.com/downloads/RavenDB%20for%20Windows%20x64/40012-Alpha c:/Raven.zip
RUN powershell -c 'mkdir c:/ravendb; expand-archive c:/raven.zip -DestinationPath c:/ravendb'
COPY vcruntime140.dll c:/ravendb/server
WORKDIR c:/ravendb/Server

CMD ./Raven.Server.exe /Raven/AllowEverybodyToAccessTheServerAsAdmin=$($env:AllowEverybodyToAccessTheServerAsAdmin) /Raven/DataDir=$($env:DataDir)
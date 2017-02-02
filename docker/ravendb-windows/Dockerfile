FROM microsoft/dotnet:1.1.0-runtime-nanoserver

COPY ./Server c:/Server

VOLUME c:/Databases
WORKDIR c:/Server
EXPOSE 8080

ENTRYPOINT ["Raven.Server.exe"]
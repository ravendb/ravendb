FROM microsoft/dotnet:1.1.0-runtime

COPY ./Server /Server

VOLUME /Databases
WORKDIR /Server
EXPOSE 8080

ENTRYPOINT ["Raven.Server"]
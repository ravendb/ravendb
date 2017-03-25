FROM ubuntu:xenial

ENV AllowEverybodyToAccessTheServerAsAdmin true
ENV DataDir /databases
VOLUME /databases
EXPOSE 8080

# Prepare container
RUN apt-get update \
    && apt-get install -y \
    && apt-get install bzip2 -y \
    && apt-get install apt-transport-https -y

# Install DotNet Core
RUN sh -c 'echo "deb [arch=amd64] https://apt-mo.trafficmanager.net/repos/dotnet-release/ xenial main" > /etc/apt/sources.list.d/dotnetdev.list' \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 417A0893 \
    && apt-get update \
    && apt-get install dotnet-dev-1.0.1 -y

ADD http://hibernatingrhinos.com/downloads/RavenDB%20for%20Ubuntu%2016.04%20x64/40010-Alpha /home/Raven.tar.bz2

RUN mkdir /home/ravendb \
    && cd /home/ravendb \
    && tar -vxjf /home/Raven.tar.bz2 \
    && rm /home/Raven.tar.bz2

WORKDIR /home/ravendb/Server

ENTRYPOINT ./Raven.Server /Raven/AllowEverybodyToAccessTheServerAsAdmin=$AllowEverybodyToAccessTheServerAsAdmin /Raven/DataDir=$DataDir

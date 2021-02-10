ARG DISTRO_VERSION
FROM ubuntu:${DISTRO_VERSION}

ARG DISTRO_VERSION
ARG DISTRO_VERSION_NAME
ARG DOTNET_RUNTIME_VERSION
ARG DOTNET_RUNTIME_DEPS


# install deps for package testing
RUN apt update \ 
    && apt install -y curl wget \
    && wget https://packages.microsoft.com/config/ubuntu/${DISTRO_VERSION}/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
    && dpkg -i ./packages-microsoft-prod.deb \
    && apt update \
    && apt install -y apt-transport-https \
    && apt update 

WORKDIR /test
ENV PACKAGE_PATH /dist/ravendb.deb
ENV DEBIAN_FRONTEND=noninteractive
ENV _DEB_DEBUG=true

ENV RAVEN_ServerUrl=http://127.0.0.1:8080
ENV RAVEN_DataDir="/var/lib/ravendb/data"
ENV RAVEN_Indexing_NugetPackagesPath="/var/lib/ravendb/nuget"
ENV RAVEN_Logs_Path="/var/log/ravendb/logs"
ENV RAVEN_Security_AuditLog_FolderPath="/var/log/ravendb/audit"
ENV RAVEN_Security_MasterKey_Path="/etc/ravendb/security/master.key"
ENV RAVEN_Setup_Certificate_Path="/etc/ravendb/security/server.pfx"
ENV HOME="/var/lib/ravendb"

COPY assets/* /assets/

CMD bash -c "source /assets/test.sh && test_package_local $PACKAGE_PATH" || (apt -qq -y install vim less > /dev/null; bash)

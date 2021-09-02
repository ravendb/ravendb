ARG DISTRO_VERSION
FROM ubuntu:${DISTRO_VERSION}

ARG DISTRO_VERSION
ARG DISTRO_VERSION_NAME
ARG DOTNET_RUNTIME_VERSION
ARG DOTNET_RUNTIME_DEPS

RUN apt update && apt install -y curl

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

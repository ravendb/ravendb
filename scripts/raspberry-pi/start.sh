#!/bin/bash
if [ $(ps -ef | grep corerun | grep Raven.Server.dll | wc -l | awk '{print $1}') -gt 0 ];
then
	printf "\nThere is already RavenDB server instance running...\n\n"
	exit 1
fi

if [ -f ravendb.5.2/Server/Raven.Server.dll ]
then
	dotnet/corerun ravendb.5.2/Server/Raven.Server.dll \
		 --Raven/RunAsService=true \
		 --Raven/ServerUrl=http://0.0.0.0:8080 \
		 --Raven/LogsDirectory=Logs \
		 --Raven/DataDir=Databases \
	 	 --Raven/StudioDirectory=ravendb.5.2/Server

else
	printf "\nRun setup.sh to install RavenDB first\n\n"
	exit 1
fi

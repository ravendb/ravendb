#!/bin/bash

CUSTOM_SETTINGS_PATH="/opt/raven-settings.json"

cd /opt/RavenDB/Server

if [ ! -f "$CUSTOM_SETTINGS_PATH" ]
then
    ./Raven.Server \
        /Raven/RunAsService=true \
        /Raven/ServerUrl/Tcp=38888 \
        /Raven/AllowEverybodyToAccessTheServerAsAdmin=${AllowEverybodyToAccessTheServerAsAdmin} \
        /Raven/DataDir=${DataDir} 
else
    ./Raven.Server \
        /Raven/RunAsService=true \
        /Raven/ServerUrl/Tcp=38888 \
        /Raven/Config=${CUSTOM_SETTINGS_PATH} \
        /Raven/DataDir=${DataDir}
fi

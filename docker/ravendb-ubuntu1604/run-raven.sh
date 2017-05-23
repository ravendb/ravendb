#!/bin/bash

CUSTOM_SETTINGS_PATH="/opt/raven-settings.json"

cd /opt/RavenDB/Server

if [ ! -f "$CUSTOM_SETTINGS_PATH" ]
then
    ./Raven.Server \
        /Raven/ServerUrl=http://0.0.0.0:8080 \
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 \
        /Raven/AllowAnonymousUserToAccessTheServer=${AllowAnonymousUserToAccessTheServer} \
        /Raven/DataDir=${DataDir} \
        --run-as-service \
        --print-id
else
    ./Raven.Server \
        /Raven/ServerUrl=http://0.0.0.0:8080 \
        /Raven/ServerUrl/Tcp=tcp://0.0.0.0:38888 \
        /Raven/DataDir=${DataDir} \
        --config-path "${CUSTOM_SETTINGS_PATH}" \
        --run-as-service \
        --print-id
fi

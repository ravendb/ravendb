#!/bin/bash

CUSTOM_SETTINGS_PATH="/opt/raven-settings.json"

cd /opt/RavenDB/Server

COMMAND="./Raven.Server"

COMMAND="$COMMAND --Security.Authentication.RequiredForPublicNetworks=false"
COMMAND="$COMMAND --ServerUrl=http://0.0.0.0:8080"
COMMAND="$COMMAND --ServerUrl.Tcp=tcp://0.0.0.0:38888"

if [ ! -z "$PublicServerUrl" ]; then
    COMMAND="$COMMAND --PublicServerUrl=$PublicServerUrl"
fi

if [ ! -z "$PublicTcpServerUrl" ]; then
    COMMAND="$COMMAND --PublicServerUrl.Tcp=$PublicTcpServerUrl"
fi

if [ ! -z "$SecurityAuthenticationEnabled" ]; then
    COMMAND="$COMMAND --Security.Authentication.Enabled=$SecurityAuthenticationEnabled"
fi

if [ ! -z "$DataDir" ]; then
    COMMAND="$COMMAND --DataDir=$DataDir"
fi

COMMAND="$COMMAND --print-id"
COMMAND="$COMMAND --daemon"

if [ ! -f "$CUSTOM_SETTINGS_PATH" ]; then
    COMMAND="$COMMAND --config-path=\"$CUSTOM_SETTINGS_PATH\""
fi

echo "Starting RavenDB server: $COMMAND"

eval $COMMAND
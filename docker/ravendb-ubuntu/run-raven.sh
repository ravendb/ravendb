#!/bin/bash
COMMAND="./Raven.Server"
export RAVEN_ServerUrl="http://$(hostname):8080"

if [ ! -z "$RAVEN_SETTINGS" ]; then
    echo "$RAVEN_SETTINGS" > settings.json
fi

if [ ! -z "$RAVEN_ARGS" ]; then
	COMMAND="$COMMAND ${RAVEN_ARGS}"
fi

if [ ! -z "$RAVEN_DATABASE" ]; then
    export RAVEN_Setup_Mode=None
    $COMMAND &
    COMMANDPID=$!
    ./wait-for-server.sh
    cert_path=$(./get-server-var.sh Configuration.Security.CertificatePath)
    if [[ "$cert_path" == "null" ]]; then
        export serverUrl=$(./get-server-url.sh)
        ./putdb.sh
    else
        export serverUrl=$(./get-server-url.sh https)
        ./cleave-cert.sh $cert_path
        ./putdb.sh /tmp/docker_cert.crt /tmp/docker_key.key
    fi
else
    $COMMAND &
    COMMANDPID=$!
fi

wait $COMMANDPID
exit $?

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
    ./putdb.sh 
else
    $COMMAND &
    COMMANDPID=$!
fi

wait $COMMANDPID
exit $?

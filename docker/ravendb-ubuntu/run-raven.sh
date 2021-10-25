#!/bin/bash
COMMAND="./Raven.Server"

RAVEN_SERVER_SCHEME="${RAVEN_SERVER_SCHEME:-http}"
export RAVEN_ServerUrl="${RAVEN_SERVER_SCHEME}://$(hostname):8080"

if [ ! -z "$RAVEN_SETTINGS" ]; then
    echo "$RAVEN_SETTINGS" > settings.json
fi

if [ ! -z "$RAVEN_ARGS" ]; then
	COMMAND="$COMMAND ${RAVEN_ARGS}"
fi

[ -n "$RAVEN_DATABASE" ] && export RAVEN_Setup_Mode=None
$COMMAND &
COMMANDPID=$!

[ -n "$RAVEN_DATABASE" ]  && source ./server-utils.sh && create-database
wait $COMMANDPID

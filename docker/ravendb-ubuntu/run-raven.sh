#!/bin/bash

# 5.x -> 6.1 migration assistance
/usr/lib/ravendb/scripts/link-legacy-datadir.sh 

COMMAND="/usr/lib/ravendb/server/Raven.Server -c /etc/ravendb/settings.json"
[ -z "$RAVEN_ServerUrl" ] && export RAVEN_ServerUrl="http://$(hostname):8080"

if [ ! -z "$RAVEN_SETTINGS" ]; then
    echo "$RAVEN_SETTINGS" > /etc/ravendb/settings.json
fi

if [ ! -z "$RAVEN_ARGS" ]; then
	COMMAND="$COMMAND ${RAVEN_ARGS}"
fi

handle_term() {
    if [ "$COMMANDPID" ]; then
        kill -TERM "$COMMANDPID" 2>/dev/null
    else
        TERM_KILL_NEEDED="yes"
    fi
}

unset COMMANDPID
unset TERM_KILL_NEEDED
trap 'handle_term' TERM INT

[ -n "$RAVEN_DATABASE" ] && export RAVEN_Setup_Mode=None
$COMMAND &
COMMANDPID=$!

[ -n "$RAVEN_DATABASE" ] && source /usr/lib/ravendb/scripts/server-utils.sh && create-database  # call to function create-database from server-utils.sh

[ "$TERM_KILL_NEEDED" ] && kill -TERM "$COMMANDPID" 2>/dev/null 
wait $COMMANDPID 2>/dev/null
trap - TERM INT
wait $COMMANDPID 2>/dev/null

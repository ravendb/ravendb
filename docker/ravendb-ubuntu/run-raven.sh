#!/bin/bash
COMMAND="./Raven.Server"

if [ -n "$RAVEN_SETTINGS" ]; then
    echo "$RAVEN_SETTINGS" > settings.json
fi

check_for_certificates() {
    if grep -q "Server.Certificate.Path" settings.json || \
       grep -q "Server.Certificate.Load.Exec" settings.json || \
       [ -n "$RAVEN_Server_Certificate_Path" ] || \
       [ -n "$RAVEN_Server_Certificate_Load_Exec" ] || \
       [[ "$RAVEN_ARGS" == *"--Server.Certificate.Path"* ]] || \
       [[ "$RAVEN_ARGS" == *"--Server.Certificate.Load.Exec"* ]]; then
        RAVEN_SERVER_SCHEME="https"
    else
        RAVEN_SERVER_SCHEME="http"
    fi
}

if [ -z "$RAVEN_ServerUrl" ]; then
    check_for_certificates
    RAVEN_ServerUrl="${RAVEN_SERVER_SCHEME}://$(hostname):8080"
    export RAVEN_ServerUrl
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

[ -n "$RAVEN_DATABASE" ]  && source ./server-utils.sh && create-database

[ "$TERM_KILL_NEEDED" ] && kill -TERM "$COMMANDPID" 2>/dev/null 
wait $COMMANDPID 2>/dev/null
trap - TERM INT
wait $COMMANDPID 2>/dev/null

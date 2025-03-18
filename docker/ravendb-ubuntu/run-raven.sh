#!/bin/bash
# ========== Defaults ==========
DEFAULT_SETTINGS_PATH="/etc/ravendb/settings.json"
RAVEN_SERVER_SCHEME="http"


# ====== Helper functions ======

# Determines server scheme (http/https)
check_for_certificates() {
    if grep -q "Server.Certificate.Path" /etc/ravendb/settings.json || \
       grep -q "Server.Certificate.Load.Exec" /etc/ravendb/settings.json || \
       [ -n "$RAVEN_Server_Certificate_Path" ] || \
       [ -n "$RAVEN_Server_Certificate_Load_Exec" ] || \
       [[ "$RAVEN_ARGS" == *"--Server.Certificate.Path"* ]] || \
       [[ "$RAVEN_ARGS" == *"--Server.Certificate.Load.Exec"* ]]; then
        RAVEN_SERVER_SCHEME="https"
    fi
}

# Determines the settings file location based on RAVEN_ARGS, using sed
# \([^ ]*\) - captures the path value, stopping at the next space or the end
# /\1/ - first path match, p - print, * - match anything around
check_for_custom_settings_path() {
    
    CUSTOM_SETTINGS_PATH=""
    if [[ "$RAVEN_ARGS" == *"-c "* ]]; then
        # Extract the path after '-c'
        CUSTOM_SETTINGS_PATH=$(echo "$RAVEN_ARGS" | sed -n 's/.*-c \([^ ]*\).*/\1/p')
    elif [[ "$RAVEN_ARGS" == *"-c="* ]]; then
        # Extract the path after '-c='
        CUSTOM_SETTINGS_PATH=$(echo "$RAVEN_ARGS" | sed -n 's/.*-c=\([^ ]*\).*/\1/p')
    elif [[ "$RAVEN_ARGS" == *"--config-path "* ]]; then
        # Extract the path after '--config-path'
        CUSTOM_SETTINGS_PATH=$(echo "$RAVEN_ARGS" | sed -n 's/.*--config-path \([^ ]*\).*/\1/p')
    elif [[ "$RAVEN_ARGS" == *"--config-path="* ]]; then
        # Extract the path after '--config-path='
        CUSTOM_SETTINGS_PATH=$(echo "$RAVEN_ARGS" | sed -n 's/.*--config-path=\([^ ]*\).*/\1/p')
    fi
}


# =========== Script ===========

# 5.x -> 6.0 migration assistance
/usr/lib/ravendb/scripts/link-legacy-datadir.sh 

check_for_custom_settings_path

COMMAND="/usr/lib/ravendb/server/Raven.Server"

# If no custom settings path found in RAVEN_ARGS, set default path.
# Otherwise, we'll add RAVEN_ARGS later, so it's no-op.
if [ -z "$CUSTOM_SETTINGS_PATH" ]; then
    COMMAND="$COMMAND -c $DEFAULT_SETTINGS_PATH"
fi

# If RAVEN_SETTINGS is set, fill the configuration file.
if [ -n "$RAVEN_SETTINGS" ]; then
    echo "$RAVEN_SETTINGS" > "${CUSTOM_SETTINGS_PATH:-$DEFAULT_SETTINGS_PATH}"
fi

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

[ -n "$RAVEN_DATABASE" ] && source /usr/lib/ravendb/scripts/server-utils.sh && create-database  # call to function create-database from server-utils.sh

[ "$TERM_KILL_NEEDED" ] && kill -TERM "$COMMANDPID" 2>/dev/null 
wait $COMMANDPID 2>/dev/null
trap - TERM INT
wait $COMMANDPID 2>/dev/null

#!/bin/bash

function get-server-var() {
# This script takes path to server variable as first arg (pass the path without . at the start)
/usr/lib/ravendb/server/rvn admin-channel <<EOF | grep -o '{"Result":[^}]*}' | jq .Result | sed 's-"--g'
script server
return server.$1
EXEC
EOF
}

function get-database-var() {
# This script takes database name as first arg and path to variable as second arg (pass the path without . at the start)
/usr/lib/ravendb/server/rvn admin-channel <<EOF | grep -o '{"Result":[^}]*}' | jq .Result | sed 's-"--g'
script database $1
return database.$2
EXEC
EOF
}
function database-exists(){
get-database-var $1 Name | grep $1
}

function get-server-url() {
# This script takes scheme as first arg (http, https, tcp etc.)
[[ $# -lt 1 ]] && scheme="http" || scheme=$1

address=$(get-server-var "ListenEndpoints.Item1[0].ToString()")
port=$(get-server-var "ListenEndpoints.Item2")

if [[ "$address" == "0.0.0.0" ]]; then
    echo $scheme://127.0.0.1:$port
elif [[ "$address" == "$(hostname)" ]]; then
    echo $scheme://$(hostname):$port
else
    echo $scheme://$address:$port
fi
}

function wait-for-server() {
    while ! (get-server-var ServerStore.Initialized | grep "true" > /dev/null); do
    sleep 0.1
    done
}

function create-database() {

    function putdb() {
        
        function putdb-usage() {
            echo "Putdb creates database on Raven.Server via HTTP(S) request, that is named after RAVEN_DATABASE environmental variable."
            echo "It should take no arguments when working with unsecured server."
            echo "In the case of secured server it should take path to .crt certificate file as first arg and path to .key file as second to work properly."
            echo "It returns 1 if cURL fails and 2 if there's wrong input."
        }

        [[ $# -eq 0 ]] && mode=unsecured || mode=secured
        if [[ $mode == unsecured ]]; then

        if curl -s $serverUrl'/admin/databases?name='$RAVEN_DATABASE -X PUT --compressed --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' > /dev/null; then 
        echo "Database '$RAVEN_DATABASE' created successfully on unsecured server."
        else
        echo "Database '$RAVEN_DATABASE' wasn't created successfully - see error output above for details."
        exit 1
        fi
        else
        if [[ $mode == secured ]]; then
        if [[ $serverUrl == "0.0.0.0" || $serverUrl == "127.0.0.1" ]]; then
        public_uri=$(get-server-var Configuration.Core.PublicServerUrl.UriValue)
        public_tcp_uri=$(get-server-var Configuration.Core.PublicTcpServerUrl.UriValue)
        curl_resolve="--resolve $public_uri:127.0.0.1 --resolve $public_tcp_uri:127.0.0.1"
        else
        curl_resolve=""
        fi

        if curl -s "$serverUrl/admin/databases?name=$RAVEN_DATABASE" -X PUT -k \
        --cert $1 \
        --key $2 \
        $curl_resolve \
        --compressed \
        --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' >/dev/null; then 
        echo "Database '$RAVEN_DATABASE' created successfully on secured server."
        else
        echo "Database '$RAVEN_DATABASE' wasn't created successfully - see error output above for details."
        exit 1
        fi

        else
        putdb-usage
        exit 2
        fi
        fi
    }

wait-for-server
if (! database-exists $RAVEN_DATABASE ); then
cert_path=$(get-server-var Configuration.Security.CertificatePath)
if [[ "$cert_path" == "null" ]]; then
    export serverUrl=$(get-server-url)
    putdb
else
    export serverUrl=$(get-server-url https)
    source ./cert-utils.sh && extract-cert-and-key $cert_path
    putdb /tmp/docker_cert.crt /tmp/docker_key.key
fi
fi
}

#!/bin/bash

function putdb() {
    
    function usage() {
        echo "Putdb creates database on Raven.Server via HTTP(S) request, that is named after RAVEN_DATABASE environmental variable."
        echo "It should take no arguments when working with unsecured server."
        echo "In the case of secured server it should take path to .crt certificate file as first arg and path to .key file as second to work properly."
        echo "It returns 1 if cURL fails and 2 if there's wrong input."
    }
    [[ $# -eq 0 ]] && mode=unsecured || mode=secured
    if [[ $mode == unsecured ]]; then
        if curl -s $serverUrl'/admin/databases?name='$RAVEN_DATABASE -X PUT --compressed --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' > /dev/null; then 
        echo "Database $RAVEN_DATABASE created successfully on unsecured server."
        else
        echo "Database $RAVEN_DATABASE wasn't created successfully - see error output above for details."
        exit 1
        fi
    else
        if [[ $mode == secured ]]; then
            public_uri=$(./get-server-var.sh Configuration.Core.PublicServerUrl.UriValue)
            public_tcp_uri=$(./get-server-var.sh Configuration.Core.PublicTcpServerUrl.UriValue)
            if curl -s "$serverUrl/admin/databases?name=$RAVEN_DATABASE" -X PUT -k \
                --cert $1 \
                --key $2 \
                --resolve $public_uri:127.0.0.1 \
                --resolve $public_tcp_uri:127.0.0.1 \
                --compressed \
                --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' >/dev/null; then 
            echo "Database $RAVEN_DATABASE created successfully on secured server."
            else
            echo "Database $RAVEN_DATABASE wasn't created successfully - see error output above for details."
            exit 1
            fi
        else
        usage
        exit 2
        fi
    fi
}


./wait-for-server.sh
cert_path=$(./get-server-var.sh Configuration.Security.CertificatePath)

if [[ "$cert_path" == "null" ]]; then
    export serverUrl=$(./get-server-url.sh)
    putdb
else
    export serverUrl=$(./get-server-url.sh https)
    ./cleave-cert.sh $cert_path
    putdb /tmp/docker_cert.crt /tmp/docker_key.key
fi

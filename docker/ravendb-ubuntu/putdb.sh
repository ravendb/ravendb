#!/bin/bash
# It takes a certificate path as first argument and key path as 2nd

if [[ $# == 0 ]]; then
    if (curl -s $serverUrl'/admin/databases?name='$RAVEN_DATABASE -X PUT --compressed --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' > /dev/null); then
    echo "Database $RAVEN_DATABASE created successfully on unsecured server."
    else
    echo "Database $RAVEN_DATABASE wasn't created successfully - unsecured server cURL error."
    exit 1
    fi
else
    if [[ $# == 2 ]]; then
        public_uri=$(./get-server-var.sh Configuration.Core.PublicServerUrl.UriValue)
        public_tcp_uri=$(./get-server-var.sh Configuration.Core.PublicTcpServerUrl.UriValue)
        if (curl -s $serverUrl'/admin/databases?name='$RAVEN_DATABASE -X PUT -k --cert $1: --key $2 --resolve $public_uri:127.0.0.1 --resolve $public_tcp_uri:127.0.0.1 -compressed --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' >/dev/null ); then 
        echo "Database $RAVEN_DATABASE created successfully on secured server."
        else
        echo "Error! Database $RAVEN_DATABASE wasn't created successfully - secured server cURL error."
        exit 1
        fi
    else
    echo "Error, please send .cert path as 1st and .key path as 2nd argument or leave it without any args for unsecured server."
    exit 2
    fi
fi
exit 0

#!/bin/bash
# This script takes scheme as first arg (http, https, tcp etc.)

[[ $# -lt 1 ]] && scheme="http" || scheme=$1

address=$(./get-server-var.sh "ListenEndpoints.Item1[0].ToString()")
port=$(./get-server-var.sh "ListenEndpoints.Item2")

if [[ "$address" == "0.0.0.0" ]]; then
    echo $scheme://127.0.0.1:$port
elif [[ "$address" == "$(hostname)" ]]; then
    echo $scheme://$(hostname):$port
else
    echo $scheme://$address:$port
fi


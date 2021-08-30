#!/bin/bash
# This script takes prefix as first arg (http, https, tcp etc.)

if [[ $# < 1 ]]; then
prefix="http"
else
prefix=$1
fi

address=$(./get-server-var.sh "ListenEndpoints.Item1[0].ToString()")
port=$(./get-server-var.sh "ListenEndpoints.Item2")

if [[ "$address" == "0.0.0.0" ]]; then
    echo $prefix://127.0.0.1:$port
elif [[ "$address" == "$(hostname)" || "$address" == "172.17.0.2" ]]; then # 172.17.0.2 is default docker address
    echo $prefix://$(hostname):$port
else
    echo $prefix://$address:$port
fi
exit 0

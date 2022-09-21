#!/bin/bash

set -e

if [ "$#" -le 1 ]; then
  echo "Wrong number of arguments."
  echo "USAGE: $0 [CLIENT_CERT_FILE] [NODE_URL]..."
  exit 1
fi

CLIENT_CERT_PFX=$1
shift

if [ ! -f "$CLIENT_CERT_PFX" ]; then
    echo "Client certificate file $CLIENT_CERT does not exit."
    exit 1
fi

if [ "$#" -lt 1 ]; then
  echo "Wrong number of arguments."
  echo "USAGE: $0 [CLIENT_CERT_FILE] [NODE_URL]..."
  exit 1
fi

NODE_URLS="$*"

KEY_FILE=$(mktemp -u "XXXXXXXXX.enc.key")
DEC_KEY_FILE=$(mktemp -u "XXXXXXXXX.key")
CRT_FILE=$(mktemp -u "XXXXXXXXX.crt")


trap "rm -f $DEC_KEY_FILE $KEY_FILE $CRT_FILE" EXIT

openssl pkcs12 -in "$CLIENT_CERT_PFX" -clcerts -nokeys -out "$CRT_FILE"
openssl pkcs12 -in "$CLIENT_CERT_PFX" -nocerts -out "$KEY_FILE"
openssl rsa -in "$KEY_FILE" -out "$DEC_KEY_FILE"

for hostname in $NODE_URLS; do

    echo "= Check connectivity..."
    if ! curl -Ss -k -X GET "$hostname/setup/alive"; then
        echo "= Failed to connect to $hostname at all. Closed firewall?"
	exit 1
    fi
    printf "= Host connectivity:\tOK\n"

    echo "= Check authentication..."
    if ! curl -Ss -v -X GET --cert "$CRT_FILE" --key "$DEC_KEY_FILE" "$hostname/admin/stats"; then
        echo "= Failed getting admin stats."
	exit 1
    fi
    echo
    printf "= Authentication information:\tOK\n"


    echo "= Reset replacement status on $hostname."
    if ! curl -v -X POST --cert "$CRT_FILE" --key "$DEC_KEY_FILE" "$hostname/admin/certificates/replacement/reset"; then
        echo "= Failed resetting replacement status. See error above"
	exit 1
    fi
    echo "= Done."

done




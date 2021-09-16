#!/bin/bash

function extract-cert-and-key() {
if [[ $# -ne 1 ]]; then
echo "Please pass certificate (e.g. .pfx) path.."
exit 1
else
source ./server-utils.sh
password=$(get-server-var Configuration.Security.CertificatePassword) | sed "s-null--"
openssl pkcs12 -in $1 -out /tmp/docker_cert.crt -nodes -nokeys -password pass:$password
openssl pkcs12 -in $1 -out /tmp/docker_key.key -nodes -nocerts -password pass:$password
fi
}

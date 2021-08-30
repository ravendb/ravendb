#!/bin/bash
if [[ $# != 1 ]]; then
echo "Please pass certificate (e.g. .pfx) path.."
exit 1
else
password=$(./get-server-var.sh Configuration.Security.CertificatePassword) | sed "s-null--"
openssl pkcs12 -in $1 -out /tmp/docker_cert.crt -nodes -nokeys -password pass:$password
openssl pkcs12 -in $1 -out /tmp/docker_key.key -nodes -nocerts -password pass:$password
fi
exit 0

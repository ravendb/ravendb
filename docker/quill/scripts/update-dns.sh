#!/bin/sh

IP="$2"
SETUP="${RAVEN_QUILL_SETUP_PACKAGE_PATH:-/var/lib/quill/setup}"

DOMAIN=$(openssl pkcs12 -in "$SETUP"/A/cluster.server.certificate.*.pfx -nokeys -passin pass: 2>/dev/null \
       | openssl x509 -noout -ext subjectAltName \
       | sed -nE 's|^ *DNS:\*\.||p')

# TODO decide weather we should include 'a' in the $IP args or it should be fixed 127.0.0.1 so it will never leave the contianer. 
exec /app/ravendb/rvn dns update -l "$SETUP/license.json" -d "$DOMAIN" \
    -n "$IP=dashboard,db,public,api"




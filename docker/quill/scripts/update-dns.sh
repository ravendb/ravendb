#!/bin/sh

IP="$2"
SETUP="${RAVEN_QUILL_SETUP_PACKAGE_PATH:-/var/lib/quill/setup}"

DOMAIN=$(openssl pkcs12 -in "$SETUP"/A/cluster.server.certificate.*.pfx -nokeys -passin pass: 2>/dev/null \
       | openssl x509 -noout -ext subjectAltName \
       | sed -nE 's|^ *DNS:\*\.||p')

# 'a' is included: every record points at the operator's IP, and the container resolves its own
# hostname to loopback via /etc/hosts (01-ravendb/run) rather than relying on DNS to do it.
exec /app/ravendb/rvn dns update -l "$SETUP/license.json" -d "$DOMAIN" \
    -n "$IP=a,dashboard,db,public,api"


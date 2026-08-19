#!/bin/sh
set -e

CERT_DIR=/var/lib/quill/certs
PROXY_DIR=/var/lib/quill/proxy
mkdir -p "$PROXY_DIR"

PFX="$(ls "$CERT_DIR"/cluster.server.certificate.*.pfx | head -n1)"

openssl pkcs12 -in "$PFX" -nokeys -clcerts -passin pass: -out "$PROXY_DIR/leaf.pem"
openssl pkcs12 -in "$PFX" -nocerts -nodes  -passin pass: -out "$PROXY_DIR/privkey.pem"
chmod 600 "$PROXY_DIR/privkey.pem"

printf '' > "$PROXY_DIR/chain.pem"
cert-chain "$PFX" "$PROXY_DIR/chain.pem" || true

cat "$PROXY_DIR/leaf.pem" "$PROXY_DIR/chain.pem" > "$PROXY_DIR/fullchain.pem"

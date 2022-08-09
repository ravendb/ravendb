#!/bin/bash


# USAGE #########################################
#
#  ./generate-selfsigned-cert.sh example.com
#
#################################################


pushd "$(dirname "$0")" > /dev/null || exit 1
SCRIPT_PATH="$(pwd)"
popd > /dev/null || exit 1

# generate a self-signed cert with proper extensions
CERT_NAME="${1:-'localhost'}"

if [ -z "$PRIVATE_KEY" ]; then
    PRIVATE_KEY="$CERT_NAME-prv.key"
fi

if [ -z "$PUBLIC_KEY" ]; then
    PUBLIC_KEY="$CERT_NAME-pub.key"
fi

if [ -z "$CRT_CERT" ]; then
    CRT_CERT="$CERT_NAME.crt"
fi

if [ -z "$PFX_CERT" ]; then
    PFX_CERT="$CERT_NAME.pfx"
fi

if [ -z "$PASSPHRASE" ]; then
    PASSPHRASE="test"
fi

CERT_CONF="$SCRIPT_PATH/cert.conf"

echo "Generate key..."
openssl genrsa -des3 \
    -passout "pass:$PASSPHRASE" \
    -out "$PRIVATE_KEY" 2048

echo "Generate cert using this key..."
openssl req -new -x509 \
    -nodes -sha1 -days 365 \
    -key "$PRIVATE_KEY" \
    -out "$CRT_CERT" \
    -passin "pass:$PASSPHRASE" \
    -extensions extensions \
    -config "$CERT_CONF" \
    -addext "subjectAltName = DNS:$CERT_NAME" \
    -subj "/C=IL/ST=Haifa/L=Hadera/O=Hibernating Rhinos/CN=$CERT_NAME"

if openssl pkcs12 -export -passout "pass:$PASSPHRASE" -passin "pass:$PASSPHRASE" -out "$PFX_CERT" -inkey "$PRIVATE_KEY" -in "$CRT_CERT"; then
    echo "Generated:"
    ls -1 | grep "$CERT_NAME"
else
    echo "Failed to generate certificate. See errors above."
fi

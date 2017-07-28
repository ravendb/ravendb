#!/bin/bash

# generate a self-signed cert with proper extensions
NAME='test'

if [ -z $PRIVATE_KEY ]; then
    PRIVATE_KEY="$NAME-prv.key"
fi

if [ -z $PUBLIC_KEY ]; then
    PUBLIC_KEY="$NAME-pub.key"
fi

if [ -z $CRT_CERT ]; then
    CRT_CERT="$NAME.crt"
fi

if [ -z $PFX_CERT ]; then
    PFX_CERT="$NAME.pfx"
fi

if [ -z $PASSPHRASE ]; then
    PASSPHRASE="test"
fi

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
    -config cert.conf \
    -subj "/C=IL/ST=Haifa/L=Hadera/O=Hibernating Rhinos/CN=test"

openssl pkcs12 -export -passout "pass:$PASSPHRASE" -passin "pass:$PASSPHRASE" -out "$PFX_CERT" -inkey "$PRIVATE_KEY" -in "$CRT_CERT"    

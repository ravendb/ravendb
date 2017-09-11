#!/bin/bash

COMMAND="./Raven.Server"

if [ ! -z "$CERTIFICATE_PATH" ]; then
    SERVER_URL_SCHEME="https"
else
    SERVER_URL_SCHEME="http"
fi

COMMAND="$COMMAND --ServerUrl=$SERVER_URL_SCHEME://0.0.0.0:8080"
COMMAND="$COMMAND --ServerUrl.Tcp=tcp://0.0.0.0:38888"

if [ ! -z "$PUBLIC_SERVER_URL" ]; then
    COMMAND="$COMMAND --PublicServerUrl=$PUBLIC_SERVER_URL"
fi

if [ ! -z "$PUBLIC_TCP_SERVER_URL" ]; then
    COMMAND="$COMMAND --PublicServerUrl.Tcp=$PUBLIC_TCP_SERVER_URL"
fi

if [ ! -z "$UNSECURED_ACCESS_ALLOWED" ]; then
    COMMAND="$COMMAND --Security.UnsecuredAccessAllowed=$UNSECURED_ACCESS_ALLOWED"
fi

if [ ! -z "$DATA_DIR" ]; then
    COMMAND="$COMMAND --DataDir=\"$DATA_DIR\""
fi

if [ ! -z "$LOGS_MODE" ]; then
    COMMAND="$COMMAND --Logs.Mode=$LOGS_MODE"
fi

CERT_PASSWORD=""

if [ ! -z "$CERTIFICATE_PASSWORD_FILE" ]; then
    CERT_PASSWORD=$(<"$CERTIFICATE_PASSWORD_FILE")
fi

if [ ! -z "$CERTIFICATE_PASSWORD" ]; then

    if [ ! -z "$CERTIFICATE_PASSWORD_FILE" ]; then
        echo "CERTIFICATE_PASSWORD and CERTIFICATE_PASSWORD_FILE cannot both be specified. Use only one of them to configure server certificate password."
        exit 1
    fi

    CERT_PASSWORD="$CERTIFICATE_PASSWORD"
fi

if [ ! -z "$CERTIFICATE_PATH" ]; then
    COMMAND="$COMMAND --Security.Certificate.Path=\"$CERTIFICATE_PATH\""

    if [ ! -d "/usr/share/ca-certificates/ravendb" ]; then
        mkdir -p /usr/share/ca-certificates/ravendb
        openssl pkcs12 -in "$CERTIFICATE_PATH" -out /tmp/server-temp.pem -password "pass:$CERT_PASSWORD" -passout "pass:" -cacerts
        openssl x509 -in /tmp/server-temp.pem -out /usr/share/ca-certificates/ravendb/server.pem
        chmod 755 /usr/share/ca-certificates/ravendb
        chmod 644 /usr/share/ca-certificates/ravendb/server.pem
        mv /usr/share/ca-certificates/ravendb/server.pem /usr/share/ca-certificates/ravendb/server.crt
        echo "ravendb/server.crt" >> /etc/ca-certificates.conf
        update-ca-certificates
    fi

fi

if [ ! -z "$CERT_PASSWORD" ]; then
    COMMAND="$COMMAND --Security.Certificate.Password=\"$CERT_PASSWORD\""
fi

COMMAND="$COMMAND --print-id"
COMMAND="$COMMAND --daemon"

if [ -f "$CUSTOM_CONFIG_FILE" ]; then
    COMMAND="$COMMAND --config-path=\"$CUSTOM_CONFIG_FILE\""
fi

echo "Starting RavenDB server: ${COMMAND/"$CERT_PASSWORD"/"*******"}"

eval $COMMAND &

./rvn logstream

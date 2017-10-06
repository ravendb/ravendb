pgrep Raven.Server > /dev/null
if [ $? -ne 0 ]; then
    echo "pgrep Raven.Server failed: $?"
    exit 1
fi

if [ ! -z "$CERTIFICATE_PATH" ]; then
    SERVER_URL_SCHEME="https"
else
    SERVER_URL_SCHEME="http"
fi

SERVER_URL="${SERVER_URL_SCHEME}://localhost:8080/studio/index.html"

if [ ! -z "$PUBLIC_SERVER_URL" ]; then
    SERVER_URL="$PUBLIC_SERVER_URL"
fi

curl -f -X GET "$SERVER_URL" > /dev/null
if [ $? -ne 0 ]; then
    echo "Could not access studio index.html with curl: $?"
    exit 1
fi

exit 0

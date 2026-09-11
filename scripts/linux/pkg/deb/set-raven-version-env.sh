if [ -z "$RAVENDB_VERSION" ]; then
    echo "Required parameter RAVENDB_VERSION is not set."
    exit 1
fi

export RAVENDB_VERSION

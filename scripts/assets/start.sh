#!/bin/bash

SCRIPT_FILE_PATH=$0
SCRIPT_DIRECTORY=`dirname $SCRIPT_FILE_PATH`
EXECUTABLE="Raven.Server"
EXECUTABLE_DIR="$SCRIPT_DIRECTORY/Server"
EXECUTABLE_PATH="$EXECUTABLE_DIR/$EXECUTABLE"
VERSION_PATH="$SCRIPT_DIRECTORY/version.txt"

ASSEMBLY_VERSION=$(eval $EXECUTABLE_PATH --version)
VERSION=""

if [[ -f "$VERSION_PATH" ]]; then
    VERSION=$(cat $VERSION_PATH);
fi

if [[ "$ASSEMBLY_VERSION" != "$VERSION" ]]; then
    echo "$ASSEMBLY_VERSION" > "$VERSION_PATH"
    xdg-open "http://ravendb.net/first-run?type=start&ver=$ASSEMBLY_VERSION";
fi

pushd $EXECUTABLE_DIR > /dev/null

sleep 2 # avoid Firefox already open warning preventing from launching the Studio tab
eval "./$EXECUTABLE --browser";

popd > /dev/null

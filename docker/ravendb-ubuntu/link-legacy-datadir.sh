#!/bin/bash
set -e

LEGACY_PATH="/opt/RavenDB/Server/RavenData"
NEW_PATH="$RAVEN_DataDir"

if [ -d "$LEGACY_PATH" ]; then
  echo "WARNING: Detected legacy data directory mount. Attempting workaround by linking $LEGACY_PATH System and Databases directories to $NEW_PATH. Please consider mounting your RavenDB data directory to $NEW_PATH"
  if [ -d "$NEW_PATH/System" ] && [ -d "$NEW_PATH/Databases" ]; then
    echo "FATAL: Found existing data under $NEW_PATH. Manual administrator assistance required."
    exit 1
  fi    

  read -r permissions username groupname <<<$(stat -c "%a %U %G" $LEGACY_PATH)

  if [[ $permissions =~ ..[0-5] ]] && \                                         # public permissions insufficent
     { [[ $groupname != "ravendb" ]] || [[ $permissions =~ .[0-5]. ]]; } && \   # group permissions insufficent
     { [[ $username != "ravendb" ]] || [[ $permissions =~ [0-5].. ]]; } then    # owner permissions insufficent
    user_uid=$(id -u)
    user_gid=$(id -g)
    ravendb_uid=$(id -u "$username")
    ravendb_gid=$(id -g "$username")
    echo "FATAL: Insufficent permissions. Please adjust permissions/ownership of $LEGACY_PATH. Read and write access is required."
    echo "       Your (UID, GID) are ($user_uid,$user_gid), but the owner $username:$groupname has (UID, GID) ($ravendb_uid,$ravendb_gid)."
    echo "       You can adjust permissions with this command: \"chmod 760 $LEGACY_PATH\"."
    echo "       You can adjust directory owner with this command: \"chown $user_uid $LEGACY_PATH\"."
    echo "       You can also adjust the group of a directory with this command: \"chown :$user_gid $LEGACY_PATH\"."

  else
    ln -sv "$LEGACY_PATH/System" "$NEW_PATH/System"
    ln -sv "$LEGACY_PATH/Databases" "$NEW_PATH/Databases"
  fi
fi

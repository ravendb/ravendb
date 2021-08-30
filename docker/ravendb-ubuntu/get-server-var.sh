#!/bin/bash
# This script takes path to server variable as first arg (pass the path without . at the start)

./rvn admin-channel <<EOF | grep -o '{"Result":[^}]*}' | jq .Result | sed 's-"--g'
script server
return server.$1
EXEC
EOF
exit 0

#!/bin/bash
while ! (netstat -a | grep LISTEN | grep 8080 > /dev/null); do
    sleep 0.1
done
if (curl -s $RAVEN_ServerUrl'/admin/databases?name='$RAVEN_DATABASE -X PUT --compressed --data-raw '{"DatabaseName":"'$RAVEN_DATABASE'"}' > /dev/null); then
echo "Database $RAVEN_DATABASE created successfully."
else
echo "Database $RAVEN_DATABASE wasn't created successfully - cURL error"
fi


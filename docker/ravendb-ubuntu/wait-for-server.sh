#!/bin/bash
while ! ( ./get-server-var.sh ServerStore.Initialized | grep "true" > /dev/null); do
    sleep 0.1
done
exit 0

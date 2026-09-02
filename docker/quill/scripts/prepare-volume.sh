#!/bin/sh
set -e

for d in ravendb certs proxy setup logs logs/ravendb-server logs/web logs/proxy logs/certwatch; do
    mkdir -p "/var/lib/quill/$d"
done

chown quill:quill /var/lib/quill
for d in certs proxy setup logs; do
    chown -R quill:quill "/var/lib/quill/$d"
done

if [ ! -f /var/lib/quill/ravendb/.ownership-v1 ]; then
    chown -R quill:quill /var/lib/quill/ravendb
    touch /var/lib/quill/ravendb/.ownership-v1
fi

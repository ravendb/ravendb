#!/bin/sh
set -e

umask 077

for d in ravendb certs proxy setup logs logs/ravendb logs/ravendb-server logs/web logs/proxy logs/certwatch; do
    mkdir -p "/var/lib/quill/$d"
done

chown quill:quill /var/lib/quill
for d in certs proxy setup; do
    chown -R quill:quill "/var/lib/quill/$d"
done

mkdir -p /run/quill
chown quill:quill /run/quill

if [ ! -f /var/lib/quill/logs/.ownership-v1 ]; then
    chown -R quill:quill /var/lib/quill/logs
    touch /var/lib/quill/logs/.ownership-v1
fi

if [ ! -f /var/lib/quill/ravendb/.ownership-v1 ]; then
    chown -R quill:quill /var/lib/quill/ravendb
    touch /var/lib/quill/ravendb/.ownership-v1
fi

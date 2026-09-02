#!/bin/sh
set -e

umask 077

DIR="/var/lib/quill/logs/$1"
mkdir -p "$DIR"

ln -sfn "$1/current" "/var/lib/quill/logs/$1.log"

exec s6-log -b n3 s10000000 T "$DIR"

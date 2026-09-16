#!/bin/sh

exec sed -u -E '
  s#(/apps/[^/ "]+/embed/[0-9a-fA-F]{6})[0-9a-fA-F]+#\1***#gI
  s#(/webhooks/slack/[0-9a-fA-F]{6})[0-9a-fA-F]+#\1***#gI
  s#(/api/apps/[^/ "]+/embed-links/[0-9a-fA-F]{6})[0-9a-fA-F]+#\1***#gI
'

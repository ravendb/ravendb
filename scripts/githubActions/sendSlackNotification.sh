#!/bin/bash

message=$(cat $MESSAGEPATH)

data='{"channel":"'$CHANNEL'","attachments":[{"mrkdwn_in":["text"],"color":"danger","text":"'$message'"}]}'

curl -H "Content-type: application/json" \
 --data "$data" \
 -H "Authorization: Bearer $SLACK_TOKEN" \
 -X POST https://slack.com/api/chat.postMessage &>/dev/null

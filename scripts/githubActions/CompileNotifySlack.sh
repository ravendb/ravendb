#!/bin/bash

prefix_x='\u274C'
prefix_check='\u2714'
prefix_question='\u2753'


status_server=$(cat pass_status_server/status_server.txt)

if [[ -e pass_status_studio/status_studio.txt ]]; then
  status_studio=$(cat pass_status_studio/status_studio.txt)
else
  status_studio="false"
fi

[[ $status_server =~ "true" && $status_studio =~ "true" ]] && exit 0


header="<${PRLINK}|[${PRTAGRETBRANCH}] [PR #${PRNUMBER}] ${PRTITLE}> failed (<$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID|GITHUB ACTIONS>)"

if [[ $status_server =~ "true" ]]; then 
  server_prefix=$prefix_check
else
  server_prefix=$prefix_x
  studio_prefix=$prefix_question
fi
server_summary="${server_prefix} Server Compilation"

if [[ $status_studio =~ "true" ]]; then 
  studio_prefix=$prefix_check
fi
studio_summary="${studio_prefix} Studio Compilation"

message="${header}\n${server_summary}\n${studio_summary}"

data='{"channel":"'$CHANNEL'","attachments":[{"mrkdwn_in":["text"],"color":"danger","text":"'$message'"}]}'

curl -H "Content-type: application/json" \
 --data "$data" \
 -H "Authorization: Bearer $TOKEN" \
 -X POST https://slack.com/api/chat.postMessage &>/dev/null 

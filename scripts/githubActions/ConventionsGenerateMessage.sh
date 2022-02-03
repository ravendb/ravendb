#!/bin/bash

function getStageSummary {
  message=$1
  resultPrefix='\u274C'
  if [[ $2 =~ "true" ]]; then
    resultPrefix='\u2714'
  fi
  echo "$resultPrefix $message"
}

status_message=$(cat pass_status_message/status_message.txt)
status_whitespace=$(cat pass_status_whitespace/status_whitespace.txt)

[[ $status_message =~ "true" && $status_whitespace =~ "true" ]] && echo "ok" > notificationMessage.txt && exit 0


header="<${PRLINK}|[${PRTARGETBRANCH}] [PR #${PRNUMBER}] ${PRTITLE}> failed (<$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID|GITHUB ACTIONS>)"
message_summary=$(getStageSummary 'Commit Message Conventions' $status_message)
whitespace_summary=$(getStageSummary 'Commit Whitespace Conventions' $status_whitespace)

message="${header}\n${message_summary}\n${whitespace_summary}"

echo $message > notificationMessage.txt

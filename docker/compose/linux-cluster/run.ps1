param([switch]$DontSetupCluster)

$ErrorActionPreference = 'Stop'

$composeCommand = "docker-compose"
$composeArgs = @()
$composeArgs += "up";
$composeArgs += "--force-recreate"
$composeArgs += "-d"

Invoke-Expression -Command "$composeCommand $composeArgs"

if ($DontSetupCluster) {
    exit 0
}

$nodes = @(
    "http://raven-node1:8080",
    "http://raven-node2:8080",
    "http://raven-node3:8080"
);

function AddNodeToCluster() {
    param($FirstNodeUrl, $OtherNodeUrl, $AssignedCores = 2)

    $otherNodeUrlEncoded = $OtherNodeUrl
    $uri = "$($FirstNodeUrl)/admin/cluster/node?url=$($otherNodeUrlEncoded)&assignedCores=$AssignedCores"
    $curlCmd = "curl -L -X PUT '$uri' -d ''"
    docker exec -it raven-node1 bash -c "$curlCmd"
    Start-Sleep -Seconds 1
}


Start-Sleep -Seconds 10 

$firstNodeIp = $nodes[0]
foreach ($node in $nodes | Select-Object -Skip 1) {
    write-Host "Add node $node to cluster";
    AddNodeToCluster -FirstNodeUrl $firstNodeIp -OtherNodeUrl $node
}

write-host "These run on Hyper-V, so they are available under one IP - usually 10.0.75.2, so:"
write-host "raven-node1 10.0.75.2:8081"
write-host "raven-node2 10.0.75.2:8082"
write-host "raven-node3 10.0.75.2:8083"

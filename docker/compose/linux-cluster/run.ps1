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
    "http://raven1:8080",
    "http://raven2:8080",
    "http://raven3:8080"
);

function AddNodeToCluster() {
    param($FirstNodeUrl, $OtherNodeUrl, $AssignedCores = 1)

    $otherNodeUrlEncoded = $OtherNodeUrl
    $uri = "$($FirstNodeUrl)/admin/cluster/node?url=$($otherNodeUrlEncoded)&assignedCores=$AssignedCores"
    $curlCmd = "curl -L -X PUT '$uri' -d ''"
    docker exec -it raven1 bash -c "$curlCmd"
    Write-Host
    Start-Sleep -Seconds 10
}


Start-Sleep -Seconds 10 

$firstNodeIp = $nodes[0]
$nodeAcoresReassigned = $false
foreach ($node in $nodes | Select-Object -Skip 1) {
    write-Host "Add node $node to cluster";
    AddNodeToCluster -FirstNodeUrl $firstNodeIp -OtherNodeUrl $node

    if ($nodeAcoresReassigned -eq $false) {
        write-host "Reassign cores on A to 1"
        $uri = "$($firstNodeIp)/admin/license/set-limit?nodeTag=A&newAssignedCores=1"
        $curlCmd = "curl -L -X POST '$uri' -d ''"
        docker exec -it raven1 bash -c "$curlCmd"
    }

}

write-host "These run on Hyper-V, so they are available under one IP - usually 10.0.75.2, so:"
write-host "raven1 10.0.75.2:8081"
write-host "raven2 10.0.75.2:8082"
write-host "raven3 10.0.75.2:8083"

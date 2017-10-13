param([switch]$DontSetupCluster, [switch]$StartBrowser)

$ErrorActionPreference = 'Stop'

$composeCommand = "docker-compose"
$composeArgs = @()
$composeArgs += "up";
$composeArgs += "--force-recreate"
$composeArgs += "-d"

Invoke-Expression -Command "$composeCommand $composeArgs"
write-host "Containers created."

$nodes = @(
    "raven-node1",
    "raven-node2",
    "raven-node3"
);

function AddNodeToCluster() {
    param($FirstNodeUrl, $OtherNodeUrl, $AssignedCores = 2)

    $otherNodeUrlEncoded = [System.Web.HttpUtility]::UrlEncode($OtherNodeUrl)
    $uri = "$($FirstNodeUrl)/admin/cluster/node?url=$($otherNodeUrlEncoded)&assignedCores=$AssignedCores"
    $curlCmd = "write-host `$(try { Invoke-WebRequest -Method PUT -Uri '$uri' } catch { write-host `$_.Exception })"
    Invoke-Expression "$curlCmd"
    Start-Sleep -Seconds 1
}

Add-Type -AssemblyName System.Web


$nodesIps = @{}
foreach ($node in $nodes) {
    $ip = docker ps -q -f name=$node | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];
    $nodesIps[$node] = $ip
    write-host "Node $node URL: http://$($ip):8080"
}

$firstNodeUrl = "http://$($nodesIps[$nodes[0]]):8080"

if ($DontSetupCluster -eq $False) {
    write-host "Setting up a cluster..."
    Start-Sleep -Seconds 5 # let nodes start
    foreach ($node in $nodes | Select-Object -Skip 1) {
        Write-Host "Add node $node to cluster";
        AddNodeToCluster -FirstNodeUrl $firstNodeUrl -OtherNodeUrl "http://$($node):8080"
    }
}

if ($StartBrowser) {
    Start-Process "$firstNodeUrl/studio/index.html#admin/settings/cluster"
}


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
    "raven1",
    "raven2",
    "raven3"
);

function DoCurl() {
    param($Method, $Uri)
    Invoke-WebRequest -Method $Method -Uri $Uri -UseBasicParsing
}

function AddNodeToCluster() {
    param($FirstNodeUrl, $OtherNodeUrl, $AssignedCores = 1)

    $otherNodeUrlEncoded = [System.Web.HttpUtility]::UrlEncode($OtherNodeUrl)
    $uri = "$($FirstNodeUrl)/admin/cluster/node?assignedCores=$AssignedCores&url=$($otherNodeUrlEncoded)"
    DoCurl -Method 'PUT' -Uri $uri
    Start-Sleep -Seconds 5
}

Add-Type -AssemblyName System.Web

$nodesIps = @{}
foreach ($node in $nodes) {
    $ip = docker ps -q -f name=$node | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ }[0];
    $nodesIps[$node] = $ip
    if ($ip -eq $null) {
        throw "Could not determine $node container's IP.";
    }
    write-host "Node $node URL: http://$($ip):8080"
}

$firstNodeUrl = "http://$($nodesIps[$nodes[0]]):8080"

$coresReassigned = $false
function ReassignCoresOnFirstNode() {
    write-host "Reassign cores on $firstNodeUrl"
    $uri = "$firstNodeUrl/admin/license/set-limit?nodeTag=A&newAssignedCores=1"
    DoCurl -Method 'POST' -Uri $uri 
}

if ($DontSetupCluster -eq $False) {
    write-host "Setting up a cluster..."

    Start-Sleep -Seconds 5 # let nodes start
    foreach ($node in $nodes | Select-Object -Skip 1) {
        Write-Host "Add node $node to cluster";
        AddNodeToCluster -FirstNodeUrl $firstNodeUrl -OtherNodeUrl "http://$($node):8080" -AssignedCores 1
        if ($coresReassigned -eq $False) {
            ReassignCoresOnFirstNode
            $coresReassigned = $true
        }
    }
}

if ($StartBrowser) {
    Start-Process "$firstNodeUrl/studio/index.html#admin/settings/cluster"
}


#Requires -RunAsAdministrator
param(
    [Parameter(Mandatory=$true)]
    $CertificatePassword="test",
    $serverDir = "C:\work\ravendb-v4.2",
    $nodeCount = 5,
    $licensePath = "C:\work\license.json"
)

$ErrorActionPreference = "Stop"

$SecurePassword = ConvertTo-SecureString $CertificatePassword -asplaintext -force

# generate server certificate 
pushd "$serverDir\scripts\certificates\powershell\" 
./generate-server-cert.ps1 -CN localhost -CertificatePassword $SecurePassword
popd 

# build 
pushd "$serverDir\src\Raven.Server\" 
$conf = "release"; 
dotnet build -c $conf 
popd 

# load servers with a certificate 
pushd "$serverDir\src\Raven.Server\bin\$conf\netcoreapp2.1" 

# clean old directoty
for($i=1; $i -le $nodeCount; $i++){
    if (Test-Path (Join-Path $(get-location) $i)) {
        Remove-Item .\$i\* -recurse
    }
} 
popd

pushd $serverDir

$commonArgs = "--Cluster.TimeBeforeAddingReplicaInSec=15" 

$authArgs = "--Security.Certificate.Path=$serverDir\scripts\certificates\powershell\server.pfx --Security.Certificate.Password=$CertificatePassword" 

for($i=1; $i -le $nodeCount; $i++){    
    start powershell "-NoExit -NoProfile dotnet run -p .\src\Raven.Server\Raven.Server.csproj --ServerUrl=https://localhost:808$i --DataDir=$serverDir\src\Raven.Server\bin\$conf\netcoreapp2.1\$i --Logs.Path=$serverDir\src\Raven.Server\bin\$conf\netcoreapp2.1\$i --License.Path=$licensePath $commonArgs $authArgs" 
    sleep -Milliseconds 500
} 
popd 

write-host "Before you continue, make sure that server https://localhost:8081 has finished loading."
sleep 3

# obtain client cert using server cert 
pushd "$serverDir\scripts\certificates\powershell\" 
./obtain-cluster-admin-client-cert.ps1 -ServerUrl https://localhost:8081 -ClientCertPassword $CertificatePassword

# add client cert to trusted root (for chrome)
$rootStore = new-object System.Security.Cryptography.X509Certificates.X509Store(
    [System.Security.Cryptography.X509Certificates.StoreName]::My, "CurrentUser")

$clientCert = Get-PfxCertificate  -FilePath ".\client-test.pfx";
$rootStore.Open("ReadWrite")

Get-ChildItem Cert:\CurrentUser\My | Where-Object {$_.Subject -eq 'CN=client-test'} | Remove-Item 
$rootStore.Add($clientCert);    
    write-host "Added self signed client certificate client-test.pfx to current user certificate store: $($clientCert.Thumbprint)" 
    
sleep 3 

# create cluster 
for($i=2; $i -le $nodeCount; $i++){
    Try
    {    
        Invoke-WebRequest -URI https://localhost:8081/admin/cluster/node?url=https://localhost:808$i -Method Put -Certificate $clientCert 
    }
    Catch
    {
        if($_.Exception.Response -ne $null) 
        {
            Write-Host $_.Exception.Message

            $stream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            Write-Host $reader.ReadToEnd()
        }
        Write-Error $_.Exception
    }
}

# create database group 
$payload = '{"DatabaseName":"Northwind","Settings":{"DataDir":null},"SecuredSettings":{},"Disabled":false,"Encrypted":false,"Topology":{"DynamicNodesDistribution":true}}' 
Try
{
    Invoke-WebRequest -Uri 'https://localhost:8081/admin/databases?name=Northwind&replication-factor=5' -Method PUT -Body $payload -Certificate $clientCert 
}
Catch
{
    if($_.Exception.Response -ne $null) 
    {
        Write-Host $_.Exception.Message

        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        Write-Host $reader.ReadToEnd()
    }
    Write-Error $_.Exception
}
popd
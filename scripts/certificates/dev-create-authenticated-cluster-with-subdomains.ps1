#Requires -RunAsAdministrator

# for this to work locally, you must edit /etc/hosts to contain the following lines:
# 127.0.0.1 rvn1.hrhinos.local
# 127.0.0.2 rvn2.hrhinos.local
# 127.0.0.3 rvn3.hrhinos.local
# 127.0.0.4 rvn4.hrhinos.local
# 127.0.0.5 rvn5.hrhinos.local
#  .
#  .
#  .

param(
    [Parameter(Mandatory=$true)]
    [SecureString]$CertificatePassword,
    $serverDir = "C:\work\ravendb-v4.0",
    $nodeCount = 5,
    $licensePath = "C:\work\license.json"
)


# generate server certificate
pushd "$serverDir\scripts\certificates\"
./generate-server-cert.ps1 -CN *.hrhinos.local -CertificatePassword $CertificatePassword
popd

# build
pushd "$serverDir\src\Raven.Server\"
$conf = "release";
dotnet build -c $conf 
popd

# load servers with a certificate
pushd "$serverDir\src\Raven.Server\bin\$conf\netcoreapp2.0"

# clean old directoty
for($i=1; $i -le $nodeCount; $i++){
    if (Test-Path (Join-Path $(get-location) $i)) {
        Remove-Item .\$i\* -recurse
    }
}

$commonArgs = "Cluster.TimeBeforeAddingReplicaInSec=15"

$authArgs = "Security.Certificate.Path=$serverDir\scripts\certificates\server.pfx Security.Certificate.Password=$CertificatePassword"

for($i=1; $i -le $nodeCount; $i++){
    start dotnet ".\Raven.Server.dll ServerUrl=https://rvn$i.hrhinos.local:8080 DataDir=$i Logs.Path=$i License.Path=$licensePath $commonArgs $authArgs"
}
popd

# obtain client cert using server cert
pushd "$serverDir\scripts\certificates\"
./obtain-cluster-admin-client-cert.ps1 -ServerUrl https://rvn1.hrhinos.local:8080


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
    Invoke-WebRequest -URI https://rvn1.hrhinos.local:8080/admin/cluster/node?url=https://rvn$i.hrhinos.local:8080 -Method Put -Certificate $clientCert
}

# create database group
$payload = '{"DatabaseName":"Northwind","Settings":{"DataDir":null},"SecuredSettings":{},"Disabled":false,"Encrypted":false,"Topology":{"DynamicNodesDistribution":true}}'
Invoke-WebRequest -Uri 'https://rvn1.hrhinos.local:8080/admin/databases?name=Northwind&replication-factor=5' -Method PUT -Body $payload -Certificate $clientCert

popd
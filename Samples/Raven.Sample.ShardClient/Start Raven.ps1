# starts the server in debug mode in the shard1 and shard2 directories, 
# meaning that they will use the specified configuration

#Set the current location to the path to the current file.
$scriptpath = $MyInvocation.MyCommand.Path
$dir = Split-Path $scriptpath
Set-Location $dir

. ..\Samples.ps1

Recreate-RavenDB Servers/Shard1 @("Raven.Bundles.Replication")
Recreate-RavenDB Servers/Shard2 @("Raven.Bundles.Replication")
Recreate-RavenDB Servers/Shard3 @("Raven.Bundles.Replication")

start .\Servers\Shard1\Raven.Server.exe --set=Raven/Port==8080
start .\Servers\Shard2\Raven.Server.exe --set=Raven/Port==8081
start .\Servers\Shard3\Raven.Server.exe --set=Raven/Port==8082
